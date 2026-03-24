//
//  IngestPayload.swift
//  TelmeIngestionService
//
//  Parses { "session": {...}, "records": [...] } and maps to ClickHouse row shapes.
//  Session is constant per session except send_mono_nanos; records may be empty.
//

import Foundation
import JSON

// MARK: - IngestPayload

/// Top-level ingest body: one session (with send_mono_nanos for this request) and a batch of records.
struct IngestPayload {
	let session: SessionRow
	let records: [RecordRow]
}

/// One row for `telme.app_sessions`. ReplacingMergeTree(send_mono_nanos) dedupes by (session_id, install_id).
struct SessionRow: Sendable {
	let sessionId: String
	let bundleId: String
	let appVersion: String
	let installId: String
	let deviceOs: String
	let deviceOsVersion: String
	let deviceHardwareModel: String
	let deviceManufacturer: String
	let baselineWallNanos: UInt64
	let baselineMonoNanos: UInt64
	let timezoneOffsetSec: Int32
	let sendMonoNanos: UInt64
}

/// One row for `telme.records`. ReplacingMergeTree(send_mono_nanos) dedupes by (session_id, record_id).
struct RecordRow: Sendable {
	let sessionId: String
	let recordId: UInt64
	let kind: String
	let eventMonoNanos: UInt64
	let recordMonoNanos: UInt64
	let sendMonoNanos: UInt64
	let eventWallNanos: UInt64
	let event: JSON
	let eventInfo: JSON
	let correlation: JSON
}

// MARK: - Parse

enum IngestParseError: Error, Sendable {
	case missingSession
	case invalidSession(String)
	case recordsNotArray
	case invalidRecord(Int, String)
}

private enum ParseUtil {
	static func parseUInt64(_ value: JSON, field: String) throws -> UInt64 {
		switch value {
		case .int(let i):
			guard i >= 0 else { throw IngestParseError.invalidSession("\(field) must be >= 0") }
			return UInt64(i)
		case .double(let d):
			guard d.isFinite, d >= 0, d.rounded(.towardZero) == d else {
				throw IngestParseError.invalidSession("\(field) must be a non-negative integer")
			}
			return UInt64(d)
		case .string(let s):
			let trimmed = s.trimmingCharacters(in: .whitespacesAndNewlines)
			guard let n = UInt64(trimmed) else {
				throw IngestParseError.invalidSession("\(field) must be a non-negative integer")
			}
			return n
		default:
			throw IngestParseError.invalidSession("\(field) must be a number")
		}
	}

	static func parseInt32(_ value: JSON, field: String) throws -> Int32 {
		switch value {
		case .int(let i):
			guard i >= Int(Int32.min), i <= Int(Int32.max) else {
				throw IngestParseError.invalidSession("\(field) out of Int32 range")
			}
			return Int32(i)
		case .double(let d):
			guard d.isFinite, d.rounded(.towardZero) == d else {
				throw IngestParseError.invalidSession("\(field) must be an integer")
			}
			guard d >= Double(Int32.min), d <= Double(Int32.max) else {
				throw IngestParseError.invalidSession("\(field) out of Int32 range")
			}
			return Int32(d)
		case .string(let s):
			let trimmed = s.trimmingCharacters(in: .whitespacesAndNewlines)
			guard let n = Int32(trimmed) else {
				throw IngestParseError.invalidSession("\(field) must be an Int32 integer")
			}
			return n
		default:
			throw IngestParseError.invalidSession("\(field) must be a number")
		}
	}
}

extension IngestPayload {
	/// Parses JSON body into session + records. Accepts snake_case (and optionally camelCase) keys.
	static func parse(_ json: JSON) throws -> IngestPayload {
		guard case .object(let top) = json else {
			throw IngestParseError.invalidSession("root must be an object")
		}
		guard let sessionJson = top["session"] else {
			throw IngestParseError.missingSession
		}
		let session = try SessionRow.parse(sessionJson)
		guard let recordsJson = top["records"] else {
			return IngestPayload(session: session, records: [])
		}
		guard case .array(let arr) = recordsJson else {
			throw IngestParseError.recordsNotArray
		}
		var records: [RecordRow] = []
		for (index, item) in arr.enumerated() {
			do {
				let row = try RecordRow.parse(item, session: session)
				records.append(row)
			} catch {
				throw IngestParseError.invalidRecord(index, String(describing: error))
			}
		}
		return IngestPayload(session: session, records: records)
	}
}

extension SessionRow {
	static func parse(_ json: JSON) throws -> SessionRow {
		guard case .object(let o) = json else {
			throw IngestParseError.invalidSession("session must be an object")
		}
		func str(_ key: String) throws -> String {
			guard let v = o[key] else { throw IngestParseError.invalidSession("session missing '\(key)'") }
			if case .string(let s) = v { return s }
			throw IngestParseError.invalidSession("session.\(key) must be string")
		}
		func u64(_ key: String) throws -> UInt64 {
			guard let v = o[key] else { throw IngestParseError.invalidSession("session missing '\(key)'") }
			return try ParseUtil.parseUInt64(v, field: "session.\(key)")
		}
		func i32(_ key: String) throws -> Int32 {
			guard let v = o[key] else { throw IngestParseError.invalidSession("session missing '\(key)'") }
			return try ParseUtil.parseInt32(v, field: "session.\(key)")
		}
		return SessionRow(
			sessionId: try str("session_id"),
			bundleId: try str("bundle_id"),
			appVersion: try str("app_version"),
			installId: try str("install_id"),
			deviceOs: try str("device_os"),
			deviceOsVersion: try str("device_os_version"),
			deviceHardwareModel: try str("device_hardware_model"),
			deviceManufacturer: try str("device_manufacturer"),
			baselineWallNanos: try u64("baseline_wall_nanos"),
			baselineMonoNanos: try u64("baseline_mono_nanos"),
			timezoneOffsetSec: try i32("timezone_offset_sec"),
			sendMonoNanos: try u64("send_mono_nanos")
		)
	}
}

extension RecordRow {
	/// Parses a record from JSON. Accepts snake_case (ingest shape) or camelCase (TelmeRecord-encoded).
	static func parse(_ recordJson: JSON, session: SessionRow) throws -> RecordRow {
		guard case .object(let o) = recordJson else {
			throw IngestParseError.invalidRecord(0, "record must be an object")
		}
		func get(_ snake: String, _ camel: String) -> JSON? {
			o[snake] ?? o[camel]
		}
		func u64(from v: JSON?, field: String) throws -> UInt64 {
			guard let v else { throw IngestParseError.invalidRecord(0, "missing \(field)") }
			do {
				return try ParseUtil.parseUInt64(v, field: field)
			} catch let e as IngestParseError {
				throw e
			} catch {
				throw IngestParseError.invalidRecord(0, "\(field) must be a non-negative integer")
			}
		}
		func u64Key(snake: String, camel: String) throws -> UInt64 {
			do {
				return try u64(from: get(snake, camel), field: "record.\(snake)")
			} catch let e as IngestParseError {
				throw e
			} catch {
				throw IngestParseError.invalidRecord(0, "record invalid '\(snake)'/'\(camel)'")
			}
		}
		func strKey(snake: String, camel: String) throws -> String {
			guard let v = get(snake, camel), case .string(let s) = v else {
				throw IngestParseError.invalidRecord(0, "record missing '\(snake)' or '\(camel)'")
			}
			return s
		}
		func jsonKey(snake: String, camel: String) throws -> JSON {
			guard let v = get(snake, camel) else {
				throw IngestParseError.invalidRecord(0, "record missing '\(snake)' or '\(camel)'")
			}
			return v
		}
		func monoNanos(from v: JSON?) -> UInt64? {
			guard let v else { return nil }
			if case .object(let obj) = v, let mono = obj["monotonic_nanos"] {
				return (try? u64(from: mono, field: "record.monotonic_nanos"))
			}
			return (try? u64(from: v, field: "record.timestamp"))
		}
		let sid = (try? strKey(snake: "session_id", camel: "sessionId")) ?? session.sessionId
		let recordId = try u64Key(snake: "record_id", camel: "recordId")
		let kind = try strKey(snake: "kind", camel: "kind")
		let eventInfoJson = get("event_info", "eventInfo")
		let timestampJson = get("timestamp", "timestamp")
		let eventMonoNanos: UInt64 = try {
			if let v = get("event_mono_nanos", "eventMonoNanos") { return try u64(from: v, field: "record.event_mono_nanos") }
			if let info = eventInfoJson, case .object(let infoObj) = info, let ts = infoObj["timestamp"] {
				if let n = monoNanos(from: ts) { return n }
			}
			throw IngestParseError.invalidRecord(0, "record missing event_mono_nanos / event_info.timestamp")
		}()
		let recordMonoNanos: UInt64 = try {
			if let v = get("record_mono_nanos", "recordMonoNanos") { return try u64(from: v, field: "record.record_mono_nanos") }
			if let n = monoNanos(from: timestampJson) { return n }
			throw IngestParseError.invalidRecord(0, "record missing record_mono_nanos / timestamp")
		}()
		let sendMonoNanos = try u64Key(snake: "send_mono_nanos", camel: "sendMonoNanos")
		let eventWallNanos: UInt64 = try {
			if let v = get("event_wall_nanos", "eventWallNanos") { return try u64(from: v, field: "record.event_wall_nanos") }
			let delta = eventMonoNanos >= session.baselineMonoNanos
				? (eventMonoNanos - session.baselineMonoNanos)
				: 0
			return session.baselineWallNanos + delta
		}()
		let event = try jsonKey(snake: "event", camel: "event")
		let eventInfo = try jsonKey(snake: "event_info", camel: "eventInfo")
		let correlation = try jsonKey(snake: "correlation", camel: "correlation")
		return RecordRow(
			sessionId: sid,
			recordId: recordId,
			kind: kind,
			eventMonoNanos: eventMonoNanos,
			recordMonoNanos: recordMonoNanos,
			sendMonoNanos: sendMonoNanos,
			eventWallNanos: eventWallNanos,
			event: event,
			eventInfo: eventInfo,
			correlation: correlation
		)
	}
}

// MARK: - JSONEachRow for ClickHouse

extension SessionRow {
	func toJSONEachRow() throws -> Data {
		let obj: JSON = .object([
			"session_id": .string(sessionId),
			"bundle_id": .string(bundleId),
			"app_version": .string(appVersion),
			"install_id": .string(installId),
			"device_os": .string(deviceOs),
			"device_os_version": .string(deviceOsVersion),
			"device_hardware_model": .string(deviceHardwareModel),
			"device_manufacturer": .string(deviceManufacturer),
			"baseline_wall_nanos": .string(String(baselineWallNanos)),
			"baseline_mono_nanos": .string(String(baselineMonoNanos)),
			"timezone_offset_sec": .int(Int(timezoneOffsetSec)),
			"send_mono_nanos": .string(String(sendMonoNanos)),
		])
		var data = try obj.toData(prettyPrinted: false)
		data.append(0x0a)
		return data
	}
}

extension RecordRow {
	func toJSONEachRow() throws -> Data {
		let obj: JSON = .object([
			"session_id": .string(sessionId),
			"record_id": .string(String(recordId)),
			"kind": .string(kind),
			"event_mono_nanos": .string(String(eventMonoNanos)),
			"record_mono_nanos": .string(String(recordMonoNanos)),
			"send_mono_nanos": .string(String(sendMonoNanos)),
			"event_wall_nanos": .string(String(eventWallNanos)),
			"event": event,
			"event_info": eventInfo,
			"correlation": correlation,
		])
		var data = try obj.toData(prettyPrinted: false)
		data.append(0x0a)
		return data
	}
}
