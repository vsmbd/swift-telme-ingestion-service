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
				let row = try RecordRow.parse(item, sessionId: session.sessionId)
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
			switch v {
			case .int(let i): return UInt64(bitPattern: Int64(i))
			case .double(let d): return UInt64(d)
			case .string(let s): return UInt64(s.trimmingCharacters(in: .whitespaces)) ?? 0
			default: throw IngestParseError.invalidSession("session.\(key) must be number")
			}
		}
		func i32(_ key: String) throws -> Int32 {
			guard let v = o[key] else { throw IngestParseError.invalidSession("session missing '\(key)'") }
			switch v {
			case .int(let i): return Int32(i)
			case .double(let d): return Int32(d)
			case .string(let s): return Int32(s) ?? 0
			default: throw IngestParseError.invalidSession("session.\(key) must be number")
			}
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
	static func parse(_ recordJson: JSON, sessionId: String) throws -> RecordRow {
		guard case .object(let o) = recordJson else {
			throw IngestParseError.invalidRecord(0, "record must be an object")
		}
		func u64(_ key: String) throws -> UInt64 {
			guard let v = o[key] else { throw IngestParseError.invalidRecord(0, "record missing '\(key)'") }
			switch v {
			case .int(let i): return UInt64(bitPattern: Int64(i))
			case .double(let d): return UInt64(d)
			case .string(let s): return UInt64(s) ?? 0
			default: throw IngestParseError.invalidRecord(0, "record.\(key) must be number")
			}
		}
		func str(_ key: String) throws -> String {
			guard let v = o[key] else { throw IngestParseError.invalidRecord(0, "record missing '\(key)'") }
			if case .string(let s) = v { return s }
			throw IngestParseError.invalidRecord(0, "record.\(key) must be string")
		}
		func json(_ key: String) throws -> JSON {
			guard let v = o[key] else { throw IngestParseError.invalidRecord(0, "record missing '\(key)'") }
			return v
		}
		let sid = (try? str("session_id")) ?? sessionId
		return RecordRow(
			sessionId: sid,
			recordId: try u64("record_id"),
			kind: try str("kind"),
			eventMonoNanos: try u64("event_mono_nanos"),
			recordMonoNanos: try u64("record_mono_nanos"),
			sendMonoNanos: try u64("send_mono_nanos"),
			eventWallNanos: try u64("event_wall_nanos"),
			event: try json("event"),
			eventInfo: try json("event_info"),
			correlation: try json("correlation")
		)
	}
}

// MARK: - JSONEachRow for ClickHouse

extension SessionRow {
	/// Single line of JSON for INSERT INTO app_sessions FORMAT JSONEachRow. UInt64 nanos sent as strings for precision.
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
		data.append(0x0a) // newline
		return data
	}
}

extension RecordRow {
	/// One line of JSON for INSERT INTO records FORMAT JSONEachRow. UInt64 nanos sent as strings for precision.
	func toJSONEachRow() throws -> Data {
		let obj: JSON = .object([
			"session_id": .string(sessionId),
			"record_id": .int(Int(truncatingIfNeeded: recordId)),
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
