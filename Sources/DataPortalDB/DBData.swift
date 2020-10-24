//
//  DataRecord.swift
//  DataPortalSQlite
//
//  Created by Bo Gustafsson on 2018-04-06.
//  Copyright © 2018 Bo Gustafsson. All rights reserved.
//

import Foundation
import SQLite


public struct DBData: Comparable, Equatable {
    public static func < (lhs: DBData, rhs: DBData) -> Bool {
        return lhs.value < rhs.value
    }
    public static func == (lhs: DBData, rhs: DBData) -> Bool {
        return lhs.value == rhs.value
    }
    
    public var id = 0
    public var stationID = 0
    public var profileID = 0
    public var depthID = 0
    public var parameterID = 0
    public var value = 0.0

    public struct TableDescription {
        public static let id = "id"
        public static let stationID = "stationID"
        public static let profileID = "profileID"
        public static let depthID = "depthID"
        public static let parameterID = "parameterID"
        public static let value = "value"
    }
    
    struct Expressions {
        static let id = Expression<Int64>(TableDescription.id)
        static let stationID = Expression<Int64>(TableDescription.stationID)
        static let profileID = Expression<Int64>(TableDescription.profileID)
        static let depthID = Expression<Int64>(TableDescription.depthID)
        static let parameterID = Expression<Int64>(TableDescription.parameterID)
        static let value = Expression<Double>(TableDescription.value)
    }
    
    public init(){}
    public init(id: Int, stationID: Int, profileID: Int, depthID: Int, parameterID: Int, value: Double) {
        self.id = id
        self.stationID = stationID
        self.profileID = profileID
        self.depthID = depthID
        self.parameterID = parameterID
        self.value = value
    }
    init(id id64: Int64, stationID stationID64: Int64, profileID profileID64: Int64, depthID depthID64: Int64, parameterID parameterID64: Int64, value: Double) {
        self.id = Int(id64)
        self.stationID = Int(stationID64)
        self.parameterID = Int(parameterID64)
        self.profileID = Int(profileID64)
        self.depthID = Int(depthID64)
        self.value = value
    }
    
    public static func createTable(dbTable: DBTable, db: Connection) throws {
         do {
            try db.run(dbTable.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.stationID)
                t.column(Expressions.profileID)
                t.column(Expressions.depthID)
                t.column(Expressions.parameterID)
                t.column(Expressions.value)
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
            try db.run(dbTable.table.createIndex(Expressions.profileID, ifNotExists: true))
        } catch {
            throw DBError.CouldNotCreateIndexError
        }
    }

    
    public mutating func insert(dbTable: DBTable, db: Connection) throws {
        let insertStatement = dbTable.table.insert(Expressions.stationID <- Int64(stationID),
                                                   Expressions.profileID <- Int64(profileID),
                                                   Expressions.depthID <- Int64(depthID),
                                                   Expressions.parameterID <- Int64(parameterID),
                                                   Expressions.value <- value)
        do {
            let rowID = try db.run(insertStatement)
            id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    public mutating func insert(db: Connection, insertStatement: Statement) throws {
        do {
            _ = try insertStatement.run(Int64(stationID), Int64(profileID), Int64(depthID), Int64(parameterID), value)
            //                id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    

    public func delete (dbTable: DBTable, db: Connection) throws -> Void {
        let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            let tmp = try db.run(query.delete())
            guard tmp == 1 else {
                throw DBError.DeleteError
            }
        } catch _ {
            throw DBError.DeleteError
        }
    }
    
    public mutating func existOrInsert(dbTable: DBTable, db: Connection) throws {
        let expression = Expressions.profileID == Int64(profileID) && Expressions.depthID == Int64(depthID) && Expressions.parameterID == Int64(parameterID)
        let query = dbTable.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
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
    public mutating func existOrInsert(dbTable: DBTable, db: Connection, insertStatement: Statement) throws {
        let expression = Expressions.profileID == Int64(profileID) && Expressions.depthID == Int64(depthID) && Expressions.parameterID == Int64(parameterID)
        let query = dbTable.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
                    self.id = id
                }
                return
            }
            do {
                try insert(db: db, insertStatement: insertStatement)
            } catch {
                throw DBError.InsertError
            }
        } catch {
            throw DBError.SearchError
        }
    }

    public static func update(id: Int, value: Double, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            let result = try db.run(query.update(Expressions.value <- value))
            return result
        } catch {
            throw DBError.UpdateError
        }
    }
    
    public static func find(id: Int, dbTable: DBTable, db: Connection) throws -> DBData? {
        let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    public static func find(profileID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID))
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    public static func find(profileID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(profileID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.parameterID == Int64(parameterID)).order(Expressions.depthID.asc)
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(profileID: Int, parameterID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
         let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.parameterID == Int64(parameterID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
// Station based search
    public static func find(stationID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(stationID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.parameterID == Int64(parameterID)).order(Expressions.depthID.asc)
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(stationID: Int, parameterID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBData] {
         let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.parameterID == Int64(parameterID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
// Find all
    public static func findAll(dbTable: DBTable, db: Connection) throws -> [DBData] {
         var retArray = [DBData]()
        do {
            let items = try db.prepare(dbTable.table)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    
// Scalar queries
    public static func findMax(profileID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> Double? {
        let command : String
        let statement : Statement
        command = "SELECT max(\(TableDescription.value)) FROM \(dbTable.tableName) WHERE (\(TableDescription.profileID) = ?) AND (\(TableDescription.parameterID) = ?)"
        statement = try! db.prepare(command)
        do {
            if let result = try statement.scalar(profileID, parameterID) as? Double {
                return result
            }
        } catch {
            throw DBError.SearchError
        }
        
        return nil
    }
    public static func findMin(profileID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> Double? {
        let command : String
        let statement : Statement
        command = "SELECT min(\(TableDescription.value)) FROM \(dbTable.tableName) WHERE (\(TableDescription.profileID) = ?) AND (\(TableDescription.parameterID) = ?)"
        statement = try! db.prepare(command)
        do {
            if let result = try statement.scalar(profileID, parameterID) as? Double {
                return result
            }
        } catch {
            throw DBError.SearchError
        }
        
        return nil
    }
    
    public static func find(profileIDRange: Range<Int>, parameterID: Int, at value: Double, with tolerance: Double, from dbTable: DBTable, and db: Connection) throws -> [DBData] {
        var retArray = [DBData]()
        let query = dbTable.table.filter(Expressions.profileID >= Int64(profileIDRange.lowerBound) && Expressions.profileID <= Int64(profileIDRange.upperBound) && Expressions.parameterID == Int64(parameterID) && Expressions.value >= value - tolerance && Expressions.value <= value + tolerance)
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], value: item[Expressions.value]))
            }
            return retArray
            
        } catch {
            throw DBError.SearchError
        }
    }
    public static func find(profileIDRange: Range<Int>, parameterID: Int, dbTable: DBTable, db: Connection) throws -> [Double] {
        var result = [Double]()
        let query = dbTable.table.filter(Expressions.profileID >= Int64(profileIDRange.lowerBound) && Expressions.profileID <= Int64(profileIDRange.upperBound) && Expressions.parameterID == Int64(parameterID))
        do {
            let items = try db.prepare(query)
            for item in items {
                result.append(item[Expressions.value])
            }
            return result
        } catch {
            throw DBError.SearchError
        }

    }

    public static func numberOfEntries(profileID: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID)).count
        do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

    public static func numberOfEntries(profileID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.depthID == Int64(depthID)).count
        do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

    
    public static func numberOfEntries(profileID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.parameterID == Int64(parameterID)).count
        do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

    
    public static func numberOfEntries(profileID: Int, parameterID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.parameterID == Int64(parameterID) && Expressions.depthID == Int64(depthID)).count
         do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

    
    public static func numberOfEntries(dbTable: DBTable, db: Connection) throws -> Int {
        do {
            let count = try db.scalar(dbTable.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

}
