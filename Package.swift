// swift-tools-version: 6.2

import PackageDescription

let package = Package(
	name: "TelmeIngestionService",
	platforms: [
		.macOS(.v11)
	],
	products: [
		.executable(
			name: "TelmeIngestionService",
			targets: ["TelmeIngestionService"]
		)
	],
	dependencies: [
		.package(
			url: "https://github.com/apple/swift-nio.git",
			from: "2.94.0"
		),
		.package(
			url: "https://github.com/vsmbd/swift-json.git",
			branch: "main"
		)
	],
	targets: [
		.executableTarget(
			name: "TelmeIngestionService",
			dependencies: [
				.product(
					name: "NIOPosix",
					package: "swift-nio"
				),
				.product(
					name: "NIOCore",
					package: "swift-nio"
				),
				.product(
					name: "NIOHTTP1",
					package: "swift-nio"
				),
				.product(
					name: "NIOConcurrencyHelpers",
					package: "swift-nio"
				),
				.product(
					name: "JSON",
					package: "swift-json"
				),
			],
			path: "Sources/TelmeIngestionService"
		)
	]
)
