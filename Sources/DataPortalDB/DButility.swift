//
//  DButility.swift
//  CocoaBoundaryConditionDBCreator
//
//  Created by Bo Gustafsson on 2017-01-18.
//  Copyright © 2017 BNI. All rights reserved.
//

import Foundation
import SQLite



struct DBTable {
    static let DataRecords = DBTable(tableName: "dataRecords")
    static let Stations = DBTable(tableName: "stations")
    static let Depths = DBTable(tableName: "depths")
    static let Profiles = DBTable(tableName: "profiles")
    static let Parameter = DBTable(tableName: "parameters")
    var tableName = String()
    var table : Table {
        get {
            return Table(tableName)
        }
    }
}

enum DBError: Error, CustomStringConvertible {
    var description: String {
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

enum DBFilehandlingError: Error, CustomStringConvertible {
    case NoFileToSaveTo
    case BackupFileCouldNotBeCreated
    case CopyInNewFileFailed
    case CopyToDiskFileFailed
    case CouldNotFindBundlePath
    case WorkingFileDoesNotExist
    var description: String {
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

class DButility {
    static let WorkingDBFilename = "dataPortal.sqlite"
    static let LastGoodFilename = "dataPortalBackup.sqlite"
    static let ResourcePath = Bundle.main.resourcePath!
    static let WorkingPath = ResourcePath + "/" + WorkingDBFilename
    static let LastGoodPath = ResourcePath + "/" + LastGoodFilename
    static let WorkingUrl = URL.init(fileURLWithPath: WorkingPath)
    static let LastGoodUrl = URL.init(fileURLWithPath: LastGoodPath)

    var db : Connection?
    init() {
        do {
            db = try Connection(DButility.WorkingPath)
        } catch {
            db = nil
        }
    }
    init?(dbFile: String) {
        do {
            db = try Connection(dbFile)
        } catch {
            return nil
        }
    }
    
    var dbTemp : Connection?
    var tempFileName : String?
    func createTemporaryConnection() throws  {
        tempFileName = DButility.ResourcePath + "/" + String.init(Date().timeIntervalSince1970)
        do {
            dbTemp = try Connection(tempFileName!)
         } catch {
            throw DBError.DatastoreConnectionError
        }
    }
    
    func moveTemporaryDBtoMain() throws {
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
    
    func copyInOpenFile(url: URL) throws {
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
    
    static func save(lastOpenedUrl: URL?) throws {
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
