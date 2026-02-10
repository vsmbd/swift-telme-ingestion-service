//
//  IngestHandler.swift
//  TelmeIngestionService
//
//  NIO HTTP handler for POST /telme/ingest: parse JSON, insert session + records to ClickHouse.
//

import Foundation
import JSON
import NIOCore
import NIOHTTP1

// MARK: - IngestHandler

final class IngestHandler: ChannelInboundHandler, @unchecked Sendable {
	typealias InboundIn = HTTPServerRequestPart
	typealias OutboundOut = HTTPServerResponsePart

	private var requestHead: HTTPRequestHead?
	private var bodyBuffer: ByteBuffer?
	private let clickHouse: ClickHouseClient
	private let maxBodySize: Int

	init(clickHouse: ClickHouseClient, maxBodySize: Int = 10 * 1024 * 1024) {
		self.clickHouse = clickHouse
		self.maxBodySize = maxBodySize
	}

	func channelRead(context: ChannelHandlerContext, data: NIOAny) {
		let part = unwrapInboundIn(data)
		switch part {
		case .head(let head):
			requestHead = head
			bodyBuffer = context.channel.allocator.buffer(capacity: 0)
		case .body(var buf):
			if var buffer = bodyBuffer {
				if buffer.readableBytes + buf.readableBytes > maxBodySize {
					Self.logResponse(status: .payloadTooLarge, body: "{\"error\":\"body too large\"}", remote: context.channel.remoteAddress)
					respond(context: context, status: .payloadTooLarge, body: "{\"error\":\"body too large\"}")
					return
				}
				buffer.writeBuffer(&buf)
				bodyBuffer = buffer
			}
		case .end:
			handleRequest(context: context)
		}
	}

	private func handleRequest(context: ChannelHandlerContext) {
		guard let head = requestHead else {
			Self.logResponse(status: .badRequest, body: "{\"error\":\"missing request\"}", remote: context.channel.remoteAddress)
			respond(context: context, status: .badRequest, body: "{\"error\":\"missing request\"}")
			return
		}
		guard head.method == .POST, head.uri == "/telme/ingest" else {
			Self.logResponse(status: .notFound, body: "{\"error\":\"not found\"}", remote: context.channel.remoteAddress)
			respond(context: context, status: .notFound, body: "{\"error\":\"not found\"}")
			return
		}
		guard let buffer = bodyBuffer, buffer.readableBytes > 0 else {
			Self.logResponse(status: .badRequest, body: "{\"error\":\"empty body\"}", remote: context.channel.remoteAddress)
			respond(context: context, status: .badRequest, body: "{\"error\":\"empty body\"}")
			return
		}
		let bodyData = Data(buffer.readableBytesView)
		requestHead = nil
		bodyBuffer = nil

		Self.logRequest(method: head.method.rawValue, uri: head.uri, bodyBytes: bodyData.count, headers: head.headers, remote: context.channel.remoteAddress)

		// Parse and insert on a task; then respond via channel (no capture of self in Task)
		let channel = context.channel
		let client = clickHouse
		Task {
			let result: (HTTPResponseStatus, String)
			do {
				let json = try JSON.parse(bodyData)
				Self.logRequestJSON(json)
				let payload = try IngestPayload.parse(json)
				Self.logPayload(sessionId: payload.session.sessionId, recordsCount: payload.records.count)
				try await client.insertAppSession(body: payload.session.toJSONEachRow())
				if !payload.records.isEmpty {
					var recordsData = Data()
					for r in payload.records {
						recordsData.append(try r.toJSONEachRow())
					}
					try await client.insertRecords(body: recordsData)
				}
				result = (.accepted, "{\"status\":\"ok\"}")
			} catch let e as IngestParseError {
				result = (.badRequest, Self.errorJSON(Self.errorMessage(e)))
			} catch let e as ClickHouseError {
				let (status, msg) = Self.clickHouseErrorResponse(e)
				result = (status, Self.errorJSON(msg))
			} catch {
				result = (.internalServerError, Self.errorJSON(error.localizedDescription))
			}
			Self.logResponse(status: result.0, body: result.1, remote: channel.remoteAddress)
			Self.respond(channel: channel, status: result.0, body: result.1)
		}
	}

	// MARK: - Logging

	private static func logRequest(method: String, uri: String, bodyBytes: Int, headers: HTTPHeaders, remote: SocketAddress?) {
		let remoteStr = remote.map { "\($0)" } ?? "?"
		let contentLength = headers["content-length"].first ?? "-"
		print("[Ingest] \(Self.timestamp()) REQUEST \(method) \(uri) remote=\(remoteStr) body_bytes=\(bodyBytes) content_length_header=\(contentLength)")
	}

	private static func logRequestJSON(_ json: JSON) {
		guard let data = try? json.toData(prettyPrinted: false),
			  let s = String(data: data, encoding: .utf8) else { return }
		print("[Ingest] \(Self.timestamp()) REQUEST_JSON \(s)")
	}

	private static func logPayload(sessionId: String, recordsCount: Int) {
		print("[Ingest] \(Self.timestamp()) PAYLOAD session_id=\(sessionId) records_count=\(recordsCount)")
	}

	private static func logResponse(status: HTTPResponseStatus, body: String, remote: SocketAddress?) {
		let remoteStr = remote.map { "\($0)" } ?? "?"
		let code = status.code
		let bodyBytes = body.utf8.count
		if code >= 400 {
			let errorSnippet = body.prefix(200).replacingOccurrences(of: "\n", with: " ")
			print("[Ingest] \(Self.timestamp()) RESPONSE status=\(code) body_bytes=\(bodyBytes) remote=\(remoteStr) error=\(errorSnippet)")
		} else {
			print("[Ingest] \(Self.timestamp()) RESPONSE status=\(code) body_bytes=\(bodyBytes) remote=\(remoteStr)")
		}
	}

	private static func timestamp() -> String {
		let formatter = ISO8601DateFormatter()
		formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
		return formatter.string(from: Date())
	}

	private func respond(context: ChannelHandlerContext, status: HTTPResponseStatus, body: String) {
		var headers = HTTPHeaders()
		headers.add(name: "Content-Type", value: "application/json")
		headers.add(name: "Content-Length", value: "\(body.utf8.count)")
		let head = HTTPResponseHead(version: .http1_1, status: status, headers: headers)
		context.write(wrapOutboundOut(.head(head)), promise: nil)
		var buf = context.channel.allocator.buffer(capacity: body.utf8.count)
		buf.writeString(body)
		context.write(wrapOutboundOut(.body(.byteBuffer(buf))), promise: nil)
		context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
	}

	private static func respond(channel: Channel, status: HTTPResponseStatus, body: String) {
		let head = HTTPResponseHead(
			version: .http1_1,
			status: status,
			headers: HTTPHeaders([
				("Content-Type", "application/json"),
				("Content-Length", "\(body.utf8.count)"),
			])
		)
		var buf = channel.allocator.buffer(capacity: body.utf8.count)
		buf.writeString(body)
		let partHead = HTTPServerResponsePart.head(head)
		let partBody = HTTPServerResponsePart.body(.byteBuffer(buf))
		let partEnd = HTTPServerResponsePart.end(nil)
		channel.pipeline.eventLoop.execute {
			_ = channel.write(partHead).flatMap { channel.write(partBody) }.flatMap { channel.writeAndFlush(partEnd) }
		}
	}

	private static func errorMessage(_ e: IngestParseError) -> String {
		switch e {
		case .missingSession: return "missing session"
		case .invalidSession(let s): return "invalid session: \(s)"
		case .recordsNotArray: return "records must be an array"
		case .invalidRecord(let i, let s): return "invalid record[\(i)]: \(s)"
		}
	}

	private static func errorJSON(_ message: String) -> String {
		let escaped = message
			.replacingOccurrences(of: "\\", with: "\\\\")
			.replacingOccurrences(of: "\"", with: "\\\"")
			.replacingOccurrences(of: "\n", with: "\\n")
		return "{\"error\":\"\(escaped)\"}"
	}

	private static func clickHouseErrorResponse(_ e: ClickHouseError) -> (HTTPResponseStatus, String) {
		switch e {
		case .httpStatus(let code, let msg):
			let status = HTTPResponseStatus(statusCode: code)
			return (status, msg ?? "ClickHouse error \(code)")
		}
	}
}
