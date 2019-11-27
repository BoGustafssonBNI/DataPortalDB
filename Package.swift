// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "DataPortalDB",
    products: [
         .library(
            name: "DataPortalDB",
            targets: ["DataPortalDB"])
    ],
    dependencies: [.package(url: "https://github.com/BoGustafssonBNI/SQLite.git", Package.Dependency.Requirement.branch("master"))],
    targets: [
        .target(
            name: "DataPortalDB",
            dependencies: ["SQLite"]),
        .testTarget(
            name: "DataPortalDBTests",
            dependencies: ["DataPortalDB"])
    ]
)
