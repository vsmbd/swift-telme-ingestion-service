//
//  ClickHouseClient.swift
//  TelmeIngestionService
//
//  HTTP INSERT to ClickHouse (database: telme). Uses URLSession.
//

import Foundation

// MARK: - ClickHouseClient

final class ClickHouseClient: @unchecked Sendable {
	private let baseURL: URL
	private let session: URLSession
	private let database: String

	/// baseURL: e.g. http://127.0.0.1:8123
	init(baseURL: URL, database: String = "telme", session: URLSession = .shared) {
		self.baseURL = baseURL
		self.database = database
		self.session = session
	}

	/// Inserts one row into telme.app_sessions. Body must be one line of JSON (JSONEachRow).
	func insertAppSession(body: Data) async throws {
		let url = insertURL(table: "app_sessions")
		var request = URLRequest(url: url)
		request.httpMethod = "POST"
		request.setValue("application/json", forHTTPHeaderField: "Content-Type")
		request.httpBody = body
		let (_, response) = try await data(for: request)
		guard let http = response as? HTTPURLResponse else { return }
		guard (200..<300).contains(http.statusCode) else {
			throw ClickHouseError.httpStatus(http.statusCode, nil)
		}
	}

	/// Inserts N rows into telme.records. Body must be newline-delimited JSON (JSONEachRow).
	func insertRecords(body: Data) async throws {
		guard !body.isEmpty else { return }
		let url = insertURL(table: "records")
		var request = URLRequest(url: url)
		request.httpMethod = "POST"
		request.setValue("application/json", forHTTPHeaderField: "Content-Type")
		request.httpBody = body
		let (data, response) = try await data(for: request)
		guard let http = response as? HTTPURLResponse else { return }
		guard (200..<300).contains(http.statusCode) else {
			let msg = data.isEmpty ? nil : String(data: data, encoding: .utf8)
			throw ClickHouseError.httpStatus(http.statusCode, msg)
		}
	}

	/// URLSession.data(for:) is macOS 12+; use completion-handler API so we support macOS 11.
	private func data(for request: URLRequest) async throws -> (Data, URLResponse) {
		try await withCheckedThrowingContinuation { continuation in
			session.dataTask(with: request) { data, response, error in
				if let error {
					continuation.resume(throwing: error)
					return
				}
				guard let data, let response else {
					continuation.resume(throwing: ClickHouseError.httpStatus(0, "missing response"))
					return
				}
				continuation.resume(returning: (data, response))
			}.resume()
		}
	}

	private func insertURL(table: String) -> URL {
		var comp = URLComponents(url: baseURL, resolvingAgainstBaseURL: false)!
		comp.queryItems = [
			URLQueryItem(name: "query", value: "INSERT INTO \(table) FORMAT JSONEachRow"),
			URLQueryItem(name: "database", value: database),
		]
		return comp.url!
	}
}

// MARK: - ClickHouseError

enum ClickHouseError: Error, Sendable {
	case httpStatus(Int, String?)
}
