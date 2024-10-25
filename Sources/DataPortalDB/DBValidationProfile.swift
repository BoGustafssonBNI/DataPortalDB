//
//  DBValidationProfile.swift
//  DataPortalDB
//
//  Created by Bo Gustafsson on 2024-10-25.
//


import Foundation
import SQLite

public struct DBValidationProfile : Comparable, Equatable, Hashable {
    public static func < (lhs: DBValidationProfile, rhs: DBValidationProfile) -> Bool {
        return lhs.date < rhs.date
    }
    public static func == (lhs: DBValidationProfile, rhs: DBValidationProfile) -> Bool {
        return lhs.date == rhs.date
    }
    public var id = 0
    public var stationID = 0
    public var timeAverageType = DBValidationProfile.TimeAverageType.Daily
    public var originatorID = 0
    public var date = Date()
    public var latitude = 0.0
    public var longitude = 0.0
    public struct TableDescription {
        public static let id = "id"
        public static let stationID = "stationID"
        public static let timeAverageType = "timeAverageType"
        public static let date = "date"
        public static let latitude = "latitude"
        public static let longitude = "longitude"
    }
    
    struct Expressions {
        static let id = SQLite.Expression<Int64>(TableDescription.id)
        static let stationID = SQLite.Expression<Int64>(TableDescription.stationID)
        static let timeAverageType = SQLite.Expression<String>(TableDescription.timeAverageType)
        static let date = SQLite.Expression<Date>(TableDescription.date)
        static let latitude = SQLite.Expression<Double>(TableDescription.latitude)
        static let longitude = SQLite.Expression<Double>(TableDescription.longitude)
    }
    
    
    public init(){}
    public init(id: Int, stationID: Int, timeAverageType: TimeAverageType, date: Date, latitude: Double, longitude: Double) {
        self.id = id
        self.stationID = stationID
        self.timeAverageType = timeAverageType
        self.date = date
        self.latitude = latitude
        self.longitude = longitude
    }
    public init(id id64: Int64, stationID stationID64: Int64, timeAverageTypeString: String, date: Date, latitude: Double, longitude: Double) {
        self.id = Int(id64)
        self.stationID = Int(stationID64)
        self.timeAverageType = TimeAverageType(rawValue: timeAverageTypeString)!
        self.date = date
        self.latitude = latitude
        self.longitude = longitude
    }
    
    public init?(from dbProfilesArray: [DBProfile], for type: TimeAverageType) {
        guard dbProfilesArray.count > 0 else { return nil }
        var dayNo = 0.0
        var latitude = 0.0
        var longitude = 0.0
        for profile in dbProfilesArray {
            dayNo += Double(profile.dayNo)
            latitude += profile.latitude
            longitude += profile.longitude
        }
        let averageLatitude = latitude / Double(dbProfilesArray.count)
        let averageLongitude = longitude / Double(dbProfilesArray.count)
        let averageDayNo = dayNo / Double(dbProfilesArray.count)
        self.id = 0
        self.stationID = dbProfilesArray.first!.stationID
        self.timeAverageType = type
        self.date = Date(timeInterval: averageDayNo * 86400, since: DBValidationProfile.ReferenceDate1850)
        self.latitude = averageLatitude
        self.longitude = averageLongitude
    }
    public enum TimeAverageType: String, CaseIterable {
        case Daily = "Daily"
        case Monthly = "Monthly"
        case Annual = "Annual"
        case Winter = "Winter"
        case Summer = "Summer"
        case Seasonal = "Seasonal"
    }

    
    public static func createTable(dbTable: DBTable, db: Connection) throws {
        do {
            try db.run(dbTable.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.stationID)
                t.column(Expressions.timeAverageType)
                t.column(Expressions.date)
                t.column(Expressions.latitude)
                t.column(Expressions.longitude)
            })
        } catch {
            throw DBError.TableCreateError
        }
    }
    public static func deleteTable(dbTable: DBTable, db: Connection) throws {
        do {
            try db.run(dbTable.table.delete())
        } catch {
            throw DBError.TableDeleteError
        }
    }
    public static func createIndex(dbTable: DBTable, db: Connection) throws {
         do {
            try db.run(dbTable.table.createIndex(Expressions.stationID, Expressions.date, ifNotExists: true))
        } catch {
            throw DBError.CouldNotCreateIndexError
        }
    }

    
    public mutating func insert(dbTable: DBTable, db: Connection) throws {
        let insertStatement = dbTable.table.insert(Expressions.stationID <- Int64(stationID),
                                                   Expressions.timeAverageType <- timeAverageType.rawValue,
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
    
    public func delete(dbTable: DBTable, db: Connection, removeData: Bool = false, dbValidationDataTable: DBTable ) throws -> Void {
        let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            try DBValidationData.delete(dbProfileID: id, dbTable: dbValidationDataTable, db: db)
            let tmp = try db.run(query.delete())
            guard tmp == 1 else {
                throw DBError.DeleteError
            }
        } catch _ {
            throw DBError.DeleteError
        }
    }
    
    public mutating func existOrInsert(dbTable: DBTable, db: Connection) throws {
        let expression = Expressions.stationID == Int64(stationID) && Expressions.timeAverageType == timeAverageType.rawValue && Expressions.date == date
        let query = dbTable.table.select(distinct: Expressions.id).filter(expression)
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
                try insert(dbTable: dbTable, db: db)
            } catch {
                throw DBError.InsertError
            }
        } catch {
            throw DBError.SearchError
        }
    }
    
    public static func find(id: Int, dbTable: DBTable, db: Connection) throws -> DBValidationProfile? {
         let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBValidationProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], timeAverageTypeString: item[Expressions.timeAverageType], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    public static func find(stationID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationProfile] {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID)).order(Expressions.date.asc)
        var retArray = [DBValidationProfile]()
        do {
            let items = try db.prepare(query)
            for item in  items {
                retArray.append(DBValidationProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], timeAverageTypeString: item[Expressions.timeAverageType], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        return retArray
    }
    
    
    public static func find(stationID: Int, minDate: Date, maxDate: Date, dbTable: DBTable, db: Connection) throws -> [DBValidationProfile] {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.date >= minDate && Expressions.date <= maxDate).order(Expressions.date.asc)
        var retArray = [DBValidationProfile]()
        do {
            let items = try db.prepare(query)
            for item in  items {
                retArray.append(DBValidationProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], timeAverageTypeString: item[Expressions.timeAverageType], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        return retArray
    }


    public static func findAll(dbTable: DBTable, db: Connection) throws -> [DBValidationProfile] {
         var retArray = [DBValidationProfile]()
        do {
            let items = try db.prepare(dbTable.table)
            for item in items {
                retArray.append(DBValidationProfile(id: item[Expressions.id], stationID: item[Expressions.stationID], timeAverageTypeString: item[Expressions.timeAverageType], date: item[Expressions.date], latitude: item[Expressions.latitude], longitude: item[Expressions.longitude]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    
    public static func numberOfEntries(dbTable: DBTable, db: Connection) throws -> Int {
        do {
            let count = try db.scalar(dbTable.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }
    public static func numberOfEntries(stationID: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID)).count
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
            return Int(self.date.timeIntervalSince(DBValidationProfile.ReferenceDate1850) / 86400.0)
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
 
}
