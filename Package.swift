// swift-tools-version:5.7
import PackageDescription

let package = Package(
    name: "DataPortalDB",
    platforms: [
        .macOS(.v13), .iOS(.v16),
    ],
    products: [
        .library(
            name: "DataPortalDB",
            targets: ["DataPortalDB"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/stephencelis/SQLite.swift.git", from: "0.17.0")
    ],
    targets: [
        .target(
            name: "DataPortalDB",
            dependencies: [
                .product(name: "SQLite", package: "SQLite.swift")
            ]
        ),
        .testTarget(
            name: "DataPortalDBTests",
            dependencies: ["DataPortalDB"]
        )
    ]
)
