//
//  main.swift
//  TelmeIngestionService
//
//  HTTP server: POST /telme/ingest → ClickHouse (database: telme).
//  Env: CLICKHOUSE_DSN (default http://127.0.0.1:8123), PORT (default 8080).
//  Optional: CLICKHOUSE_USER, CLICKHOUSE_PASSWORD for Basic auth.
//  Optional: CLICKHOUSE_ASYNC_INSERT (default 1), CLICKHOUSE_WAIT_FOR_ASYNC_INSERT (default 1).
//

import Foundation
import NIOCore
import NIOHTTP1
import NIOPosix

let clickHouseDSN = ProcessInfo.processInfo.environment["CLICKHOUSE_DSN"] ?? "http://127.0.0.1:8123"
let port = Int(ProcessInfo.processInfo.environment["PORT"] ?? "8080") ?? 8080
let clickHouseUser = ProcessInfo.processInfo.environment["CLICKHOUSE_USER"]
let clickHousePassword = ProcessInfo.processInfo.environment["CLICKHOUSE_PASSWORD"]
let asyncInsert = (ProcessInfo.processInfo.environment["CLICKHOUSE_ASYNC_INSERT"] ?? "1") != "0"
let waitForAsyncInsert = (ProcessInfo.processInfo.environment["CLICKHOUSE_WAIT_FOR_ASYNC_INSERT"] ?? "1") != "0"

guard let baseURL = URL(string: clickHouseDSN) else {
	fatalError("Invalid CLICKHOUSE_DSN: \(clickHouseDSN)")
}

let clickHouse = ClickHouseClient(
	baseURL: baseURL,
	database: "telme",
	user: clickHouseUser,
	password: clickHousePassword,
	asyncInsert: asyncInsert,
	waitForAsyncInsert: waitForAsyncInsert
)

let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
defer { try? group.syncShutdownGracefully() }

let bootstrap = ServerBootstrap(group: group)
	.serverChannelOption(ChannelOptions.backlog, value: 256)
	.serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
	.childChannelInitializer { channel in
		let handler = IngestHandler(clickHouse: clickHouse)
		return channel.pipeline.configureHTTPServerPipeline(withErrorHandling: true).flatMap {
			channel.pipeline.addHandler(handler)
		}
	}
	.childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)

do {
	let channel = try bootstrap.bind(host: "0.0.0.0", port: port).wait()
	guard let addr = channel.localAddress else { fatalError("could not get local address") }
	print("TelmeIngestionService listening on \(addr), ClickHouse at \(clickHouseDSN), database telme")
	try channel.closeFuture.wait()
} catch {
	fatalError("Failed to start server: \(error)")
}
