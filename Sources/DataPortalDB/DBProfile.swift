//
//  Profile.swift
//  DataPortalSQlite
//
//  Created by Bo Gustafsson on 2018-04-06.
//  Copyright © 2018 Bo Gustafsson. All rights reserved.
//

import Foundation
import SQLite

public struct DBProfile : Comparable, Equatable, Hashable {
    public static func < (lhs: DBProfile, rhs: DBProfile) -> Bool {
        return lhs.date < rhs.date
    }
    public static func == (lhs: DBProfile, rhs: DBProfile) -> Bool {
        return lhs.date == rhs.date
    }
    public var id = 0
    public var stationID = 0
    public var serverID = ""
    public var originatorID = 0
    public var date = Date()
    public var latitude = 0.0
    public var longitude = 0.0
    public struct TableDescription {
        public static let id = "id"
        public static let stationID = "stationID"
        public static let serverID = "serverID"
        public static let originatorID = "originatorID"
        public static let date = "date"
        public static let latitude = "latitude"
        public static let longitude = "longitude"
    }
    
    struct Expressions {
        static let id = Expression<Int64>(TableDescription.id)
        static let stationID = Expression<Int64>(TableDescription.stationID)
        static let serverID = Expression<String>(TableDescription.serverID)
        static let originatorID = Expression<Int64>(TableDescription.originatorID)
        static let date = Expression<Date>(TableDescription.date)
        static let latitude = Expression<Double>(TableDescription.latitude)
        static let longitude = Expression<Double>(TableDescription.longitude)
    }
    
    
    public init(){}
    public init(id: Int, stationID: Int, cruise: String, stationNumber: Int, date: Date, latitude: Double, longitude: Double) {
        self.id = id
        self.stationID = stationID
        self.serverID = cruise
        self.originatorID = stationNumber
        self.date = date
        self.latitude = latitude
        self.longitude = longitude
    }
    public init(id: Int, stationID: Int, serverID: String, originatorID: Int, date: Date, latitude: Double, longitude: Double) {
        self.id = id
        self.stationID = stationID
        self.serverID = serverID
        self.originatorID = originatorID
        self.date = date
        self.latitude = latitude
        self.longitude = longitude
    }
    public init(id id64: Int64, stationID stationID64: Int64, serverID: String, originatorID originatorID64: Int64, date: Date, latitude: Double, longitude: Double) {
        self.id = Int(id64)
        self.stationID = Int(stationID64)
        self.serverID = serverID
        self.originatorID = Int(originatorID64)
        self.date = date
        self.latitude = latitude
        self.longitude = longitude    }
    
    public static func createTable(db: Connection) throws {
        do {
            try db.run(DBTable.Profiles.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.stationID)
                t.column(Expressions.serverID)
                t.column(Expressions.originatorID)
                t.column(Expressions.date)
                t.column(Expressions.latitude)
                t.column(Expressions.longitude)
            })
        } catch {
            throw DBError.TableCreateError
        }
    }
    public static func deleteTable(db: Connection) throws {
        do {
            try db.run(DBTable.Profiles.table.delete())
        } catch {
            throw DBError.TableDeleteError
        }
    }
    public static func createIndex(db: Connection) throws {
         do {
            try db.run(DBTable.Profiles.table.createIndex(Expressions.stationID, Expressions.date, ifNotExists: true))
        } catch {
            throw DBError.CouldNotCreateIndexError
        }
    }

    
    public mutating func insert(db: Connection) throws {
        let insertStatement = DBTable.Profiles.table.insert(Expressions.stationID <- Int64(stationID),
                                                           Expressions.serverID <- serverID,
                                                           Expressions.originatorID <- Int64(originatorID),
                                                           Expressions.date <- date,
                                                           Expressions.latitude <- latitude,
                                                           Expressions.longitude <- longitude)
        do {
            let rowID = try db.run(insertStatement)
            id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    
    public func delete(db: Connection, removeData: Bool = false, dbDataTable: DBTable = DBTable.DataRecords) throws -> Void {
        let query = DBTable.Profiles.table.filter(Expressions.id == Int64(id))
        do {
            try DBData.delete(dbProfileID: id, dbTable: dbDataTable, db: db)
            let tmp = try db.run(query.delete())
            guard tmp == 1 else {
                throw DBError.DeleteError
            }
        } catch _ {
            throw DBError.DeleteError
        }
    }
    
    public mutating func existOrInsert(db: Connection) throws {
        let expression = Expressions.stationID == Int64(stationID) && Expressions.serverID == serverID && Expressions.originatorID == Int64(originatorID) && Expressions.date == date
        let query = DBTable.Profiles.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
                    print("Already in DB: Profile id = \(self.id) db id = \(id)")
                    self.id = id
                }
                return
            }
            do {
                try insert(db: db)
            } catch {
                throw DBError.InsertError
            }
        } catch {
            throw DBError.SearchError
        }
    }
    
    public static func find(id: Int, db: Connection) throws -> DBProfile? {
         let query = DBTable.Profiles.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], serverID: item[Expressions.serverID], originatorID: item[Expressions.originatorID], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    public static func find(stationID: Int, db: Connection) throws -> [DBProfile] {
        let query = DBTable.Profiles.table.filter(Expressions.stationID == Int64(stationID)).order(Expressions.date.asc)
        var retArray = [DBProfile]()
        do {
            let items = try db.prepare(query)
            for item in  items {
                retArray.append(DBProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], serverID: item[Expressions.serverID], originatorID: item[Expressions.originatorID], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        return retArray
    }
    public static func find(stationID: Int, minDate: Date, maxDate: Date, db: Connection) throws -> [DBProfile] {
        let query = DBTable.Profiles.table.filter(Expressions.stationID == Int64(stationID) && Expressions.date >= minDate && Expressions.date <= maxDate).order(Expressions.date.asc)
        var retArray = [DBProfile]()
        do {
            let items = try db.prepare(query)
            for item in  items {
                retArray.append(DBProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], serverID: item[Expressions.serverID], originatorID: item[Expressions.originatorID], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        return retArray
    }

    public static func find(stationID: Int, cruise: String, stationNumber: Int, date: Date, db: Connection) throws -> [DBProfile] {
        let minDate = date.addingTimeInterval(-43200.0)
        let maxDate = date.addingTimeInterval(43200.0)
        let query = DBTable.Profiles.table.filter(Expressions.stationID == Int64(stationID) && Expressions.date >= minDate && Expressions.date <= maxDate && Expressions.serverID == cruise && Expressions.originatorID == Int64(stationNumber)).order(Expressions.date.asc)
        var retArray = [DBProfile]()
        do {
            let items = try db.prepare(query)
            for item in  items {
                retArray.append(DBProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], serverID: item[Expressions.serverID], originatorID: item[Expressions.originatorID], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        return retArray
    }

    public static func findAll(db: Connection) throws -> [DBProfile] {
         var retArray = [DBProfile]()
        do {
            let items = try db.prepare(DBTable.Profiles.table)
            for item in items {
                retArray.append(DBProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], serverID: item[Expressions.serverID], originatorID: item[Expressions.originatorID], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    
    public static func numberOfEntries(db: Connection) throws -> Int {
        do {
            let count = try db.scalar(DBTable.Profiles.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }
    public static func numberOfEntries(stationID: Int, db: Connection) throws -> Int {
        let query = DBTable.Profiles.table.filter(Expressions.stationID == Int64(stationID)).count
        do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

}
