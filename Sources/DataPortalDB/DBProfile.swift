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
    
    public func delete(db: Connection, removeData: Bool = false, dbDataTable: DBTable = DBTable.DataRecords, dbSDataTable: DBTable? = nil, dbODataTable: DBTable? = nil ) throws -> Void {
        let query = DBTable.Profiles.table.filter(Expressions.id == Int64(id))
        do {
            try DBData.delete(dbProfileID: id, dbTable: dbDataTable, db: db)
            if let dbSDataTable = dbSDataTable {
                try DBSData.delete(dbProfileID: id, dbTable: dbSDataTable, db: db)
            }
            if let dbODataTable = dbODataTable {
                try DBOData.delete(dbProfileID: id, dbTable: dbODataTable, db: db)
            }
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
    
    public static func find(stationID: Int, for timeAverageType: DBProfile.TimeAverageTypes, db: Connection) throws -> [[DBProfile]] {
        let query = DBTable.Profiles.table.filter(Expressions.stationID == Int64(stationID)).order(Expressions.date.asc)
        var profiles = [DBProfile]()
        do {
            let items = try db.prepare(query)
            for item in  items {
                profiles.append(DBProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], serverID: item[Expressions.serverID], originatorID: item[Expressions.originatorID], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        return profiles.toAverage(for: timeAverageType)
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
    
    public static func find(stationID: Int, date: Date, timeInterval: Double = 43200.0, latitude: Double, latitudeInterval: Double = 0.0085, longitude: Double, longitudeInterval: Double = 0.0085, db: Connection) throws -> [DBProfile] {
        let minDate = date.addingTimeInterval(-timeInterval)
        let maxDate = date.addingTimeInterval(timeInterval)
        let latMin = latitude - latitudeInterval
        let latMax = latitude + latitudeInterval
        let lonMin = longitude - longitudeInterval
        let lonMax = longitude + longitudeInterval
        let query = DBTable.Profiles.table.filter(Expressions.stationID == Int64(stationID) && Expressions.date >= minDate && Expressions.date <= maxDate && Expressions.latitude >= latMin && Expressions.latitude <= latMax  && Expressions.longitude >= lonMin && Expressions.longitude <= lonMax).order(Expressions.date.asc)
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
    private static let ReferenceDate1850 = DateComponents(calendar: Calendar.UTCCalendar, year: 1849, month: 12, day: 31, hour: 0, minute: 0, second: 0).date!
    
    public var dayNo : Int {
        get {
            return Int(self.date.timeIntervalSince(DBProfile.ReferenceDate1850) / 86400.0)
        }
    }
    public var ymd : [Int] {
        get {
            let year = Calendar.UTCCalendar.component(.year, from: self.date)
            let month = Calendar.UTCCalendar.component(.month, from: self.date)
            let day = Calendar.UTCCalendar.component(.day, from: self.date)
            return [year, month, day]
        }
    }
    public var year : Int {
        get {
            return Calendar.UTCCalendar.component(.year, from: self.date)
        }
    }
    public var month : Int {
        get {
            return Calendar.UTCCalendar.component(.month, from: self.date)
        }
    }
    public var day : Int {
        get {
            return Calendar.UTCCalendar.component(.day, from: self.date)
        }
    }
    public var decimalYear : Double {
        get {
            return Double(dayNo) / 365.245 + 1850
        }
    }
    
    public enum TimeAverageTypes: Equatable, Hashable {
        public static func allCases(annualInterval : Int = 1, monthlyInterval : Int = 1, dailyInterval : Int = 1) -> [DBProfile.TimeAverageTypes] {
                return [.All, .Seasonal, .Annual(interval: 1), .Monthly(interval: 1), .Daily(interval: 1), .Winter(startMonth: 12, endMonth: 2), .Summer(startMonth: 6, endMonth: 9)]
        }
        case Daily(interval: Int = 1)
        case Monthly(interval: Int = 1)
        case Annual(interval: Int = 1)
        case Winter(startMonth: Int = 12, endMonth: Int = 2)
        case Summer(startMonth: Int = 6, endMonth: Int = 9)
        case Seasonal
        case All
        public var description : String {
            get {
                switch self {
                case .Daily(let dayInterval):
                    return dayInterval == 1 ? "Daily" : "\(dayInterval) days"
                case .Monthly:
                    return "Monthly"
                case .Annual(let yearInterval):
                    return yearInterval == 1 ? "Annual" : "\(yearInterval) years"
                case .Winter(let startMonth, let endMonth):
                    return "Winter (\(startMonth) - \(endMonth))"
                case .Summer(let startMonth, let endMonth):
                    return "Summer (\(startMonth) - \(endMonth))"
                case .Seasonal:
                    return "Seasonal"
                case .All:
                    return "All"
                }
            }
        }
    }

}
extension Calendar {
    internal static var UTCCalendar : Calendar {
        get {
            var cal = Calendar.init(identifier: .iso8601)
            cal.timeZone = TimeZone(identifier: "UTC")!
            return cal
        }
    }
}
