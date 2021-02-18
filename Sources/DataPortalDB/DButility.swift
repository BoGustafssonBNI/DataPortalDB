//
//  DButility.swift
//  CocoaBoundaryConditionDBCreator
//
//  Created by Bo Gustafsson on 2017-01-18.
//  Copyright © 2017 BNI. All rights reserved.
//

import Foundation
import SQLite



public struct DBTable {
    public static let DataRecords = DBTable(tableName: "dataRecords")
    public static let SDataRecords = DBTable(tableName: "SalinitySortedDataRecords")
    public static let Stations = DBTable(tableName: "stations")
    public static let Depths = DBTable(tableName: "depths")
    public static let Salinities = DBTable(tableName: "salinities")
    public static let Profiles = DBTable(tableName: "profiles")
    public static let Parameter = DBTable(tableName: "parameters")
    public var tableName = String()
    public var table : Table {
        get {
            return Table(tableName)
        }
    }
}

public enum DBError: Error, CustomStringConvertible {
    public var description: String {
        get {
            switch self {
            case .DatastoreConnectionError:
                return "DB Data store connection error"
            case .DeleteError:
                return "DB Delete error"
            case .InsertError:
                return "DB Insert error"
            case .SearchError:
                return "DB Seach error"
            case .TableCreateError:
                return "DB Table create error"
            case .TableDeleteError:
                return "DB Table delete error"
            case .UpdateError:
                return "DB Update error"
            case .TableNilError:
                return "Table does not exist"
            case .CouldNotCreateIndexError:
                return "Could not create index"
            }
        }
    }
    case DatastoreConnectionError
    case TableCreateError
    case TableDeleteError
    case InsertError
    case DeleteError
    case SearchError
    case UpdateError
    case TableNilError
    case CouldNotCreateIndexError
}

public enum DBFilehandlingError: Error, CustomStringConvertible {
    case NoFileToSaveTo
    case BackupFileCouldNotBeCreated
    case CopyInNewFileFailed
    case CopyToDiskFileFailed
    case CouldNotFindBundlePath
    case WorkingFileDoesNotExist
    public var description: String {
        get {
            switch self {
            case .NoFileToSaveTo:
                return "No file to save to"
            case .CopyInNewFileFailed:
                return "File could not be copied into bundle"
            case .CopyToDiskFileFailed:
                return "Working file could not be copied to diskfile"
            case .BackupFileCouldNotBeCreated:
                return "Working file could not be copied to backupfile"
            case .CouldNotFindBundlePath:
                return "Failed to find Bundle path"
            case .WorkingFileDoesNotExist:
                return "Working file does not exist"
            }
        }
    }
}

public class DButility {
    public static let WorkingDBFilename = "dataPortal.sqlite"
    public static let LastGoodFilename = "dataPortalBackup.sqlite"
    static let ResourcePath = Bundle.main.resourcePath!
    public static let WorkingPath = ResourcePath + "/" + WorkingDBFilename
    public static let LastGoodPath = ResourcePath + "/" + LastGoodFilename
    public static let WorkingUrl = URL.init(fileURLWithPath: WorkingPath)
    public static let LastGoodUrl = URL.init(fileURLWithPath: LastGoodPath)

    public var db : Connection?
    public init() {
        do {
            db = try Connection(DButility.WorkingPath)
        } catch {
            db = nil
        }
    }
    public init?(dbFile: String) {
        do {
            db = try Connection(dbFile)
        } catch {
            return nil
        }
    }
    
    public var dbTemp : Connection?
    public var tempFileName : String?
    public func createTemporaryConnection() throws  {
        tempFileName = DButility.ResourcePath + "/" + String.init(Date().timeIntervalSince1970)
        do {
            dbTemp = try Connection(tempFileName!)
         } catch {
            throw DBError.DatastoreConnectionError
        }
    }
    
    public func moveTemporaryDBtoMain() throws {
        if dbTemp != nil, let file = tempFileName {
            let semaphore = DispatchSemaphore(value: 1)
            semaphore.wait()
            db!.interrupt()
            db = nil
            dbTemp!.interrupt()
            dbTemp = nil
            do {
                if let resultingUrl = try FileManager.default.replaceItemAt(DButility.WorkingUrl, withItemAt: URL.init(fileURLWithPath: file)) {
                    print("Copied to: \(resultingUrl)")
                }
                db = try Connection(DButility.WorkingPath)
                semaphore.signal()
            } catch {
                print(error)
                do {
                    db = try Connection(DButility.WorkingPath)
                    semaphore.signal()
                } catch {
                    print("DB Could not be reconnected for failed with \(error) during copy in")
                    semaphore.signal()
                    throw error
                }
                throw DBFilehandlingError.CopyInNewFileFailed
            }
        }
    }
    
    public func copyInOpenFile(url: URL) throws {
        let semaphore = DispatchSemaphore(value: 1)
        semaphore.wait()
        db!.interrupt()
        db = nil
        do {
            let path = url.path
            let backupFile = DButility.ResourcePath + "/" + String.init(Date().timeIntervalSince1970)
            let backupUrl = URL.init(fileURLWithPath: backupFile)
            try FileManager.default.copyItem(atPath: path, toPath: backupFile)
            if let resultingUrl = try FileManager.default.replaceItemAt(DButility.WorkingUrl, withItemAt: backupUrl) {
                print("Copied to: \(resultingUrl)")
            }
            db = try Connection(DButility.WorkingPath)
            semaphore.signal()
        } catch {
            print(error)
            do {
                db = try Connection(DButility.WorkingPath)
                semaphore.signal()
            } catch {
                print("DB Could not be reconnected for failed with \(error) during copy in")
                semaphore.signal()
                throw error
            }
            throw DBFilehandlingError.CopyInNewFileFailed
        }
    }
    
    public static func save(lastOpenedUrl: URL?) throws {
        if let last = lastOpenedUrl {
            do {
                let workingUrl = URL.init(fileURLWithPath: DButility.WorkingPath)
                if FileManager.default.fileExists(atPath: last.path) {
                    let temp = last.path + String.init(Date().timeIntervalSince1970)
                    try FileManager.default.copyItem(atPath: last.path, toPath: temp)
                    try FileManager.default.removeItem(at: last)
                    try FileManager.default.copyItem(at: workingUrl, to: last)
                    try FileManager.default.removeItem(atPath: temp)
                } else {
                    try FileManager.default.copyItem(at: workingUrl, to: last)
                }
            } catch {
                throw DBFilehandlingError.CopyToDiskFileFailed
            }
        } else {
            throw DBFilehandlingError.NoFileToSaveTo
        }
    }
}
