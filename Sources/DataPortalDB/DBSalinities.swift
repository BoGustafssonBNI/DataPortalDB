//
//  DBSalinities.swift
//
//
//  Created by Bo Gustafsson on 2021-02-18.
//

import Foundation
import SQLite


public struct DBSalinities: Comparable, Equatable, Hashable {
    public static func < (lhs: DBSalinities, rhs: DBSalinities) -> Bool {
        return lhs.value < rhs.value
    }
    public static func == (lhs: DBSalinities, rhs: DBSalinities) -> Bool {
        return lhs.value == rhs.value
    }
    public var id = 0
    public var stationID = 0
    public var level = 0
    public var value = 0.0
    public struct TableDescription {
        public static let id = "id"
        public static let stationID = "stationID"
        public static let level = "level"
        public static let value = "value"
    }
    
    struct Expressions {
        static let id = SQLite.Expression<Int64>(TableDescription.id)
        static let stationID = SQLite.Expression<Int64>(TableDescription.stationID)
        static let level = SQLite.Expression<Int64>(TableDescription.level)
        static let value = SQLite.Expression<Double>(TableDescription.value)
    }
    
    public init(){}
    public init(id: Int, stationID: Int, level: Int, value: Double) {
        self.id = id
        self.stationID = stationID
        self.level = level
        self.value = value
    }
    public init(id id64: Int64, stationID stationID64: Int64, level level64: Int64, value: Double) {
        self.id = Int(id64)
        self.stationID = Int(stationID64)
        self.level = Int(level64)
        self.value = value
    }
    
    public static func createTable(db: Connection) throws {
         do {
            try db.run(DBTable.Salinities.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.stationID)
                t.column(Expressions.level)
                t.column(Expressions.value)
            })
        } catch {
            throw DBError.TableCreateError
        }
    }
    public static func deleteTable(db: Connection) throws {
        do {
            try db.run(DBTable.Salinities.table.delete())
        } catch {
            throw DBError.TableDeleteError
        }
    }

    public static func createIndex(dbTable: DBTable, db: Connection) throws {
         do {
            try db.run(dbTable.table.createIndex(Expressions.stationID, ifNotExists: true))
        } catch {
            throw DBError.CouldNotCreateIndexError
        }
    }

    
    public mutating func insert(db: Connection) throws {
        let insertStatement = DBTable.Salinities.table.insert(Expressions.stationID <- Int64(stationID),
                                                          Expressions.level <- Int64(level),
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
            _ = try insertStatement.run(Int64(stationID), Int64(level), value)
            //                id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    

    public func delete (db: Connection) throws -> Void {
        let query =  DBTable.Salinities.table.filter(Expressions.id == Int64(id))
        do {
            let tmp = try db.run(query.delete())
            guard tmp == 1 else {
                throw DBError.DeleteError
            }
        } catch _ {
            throw DBError.DeleteError
        }
    }
    
    public mutating func existOrInsert(db: Connection) throws {
        let expression = Expressions.stationID == Int64(stationID) && Expressions.level == Int64(level)
        let query =  DBTable.Salinities.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
                    //                        print("Data already in DB: Data id = \(self.id) db id = \(id)")
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
    public mutating func existOrInsert(db: Connection, insertStatement: Statement) throws {
        let expression = Expressions.stationID == Int64(stationID) && Expressions.level == Int64(level)
        let query =  DBTable.Salinities.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
                    //                        print("Data already in DB: Data id = \(self.id) db id = \(id)")
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
    
    public static func update(id: Int, value: Double, db: Connection) throws -> Int {
        let query =  DBTable.Salinities.table.filter(Expressions.id == Int64(id))
        do {
            let result = try db.run(query.update(Expressions.value <- value))
            return result
        } catch {
            throw DBError.UpdateError
        }
    }

    public static func find(id: Int, db: Connection) throws -> DBSalinities? {
        let query =  DBTable.Salinities.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBSalinities(id: item[Expressions.id], stationID: item[Expressions.stationID], level: item[Expressions.level], value: item[Expressions.value])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    public static func find(stationID: Int, db: Connection) throws -> [DBSalinities] {
         let query =  DBTable.Salinities.table.filter(Expressions.stationID == Int64(stationID))
        var retArray = [DBSalinities]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBSalinities(id: item[Expressions.id], stationID: item[Expressions.stationID], level: item[Expressions.level], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    public static func find(stationID: Int, level: Int, db: Connection) throws -> [DBSalinities] {
        let query =  DBTable.Salinities.table.filter(Expressions.stationID == Int64(stationID) && Expressions.level == Int64(level))
        var retArray = [DBSalinities]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBSalinities(id: item[Expressions.id], stationID: item[Expressions.stationID], level: item[Expressions.level], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }


    public static func findAll(db: Connection) throws -> [DBSalinities] {
         var retArray = [DBSalinities]()
        do {
            let items = try db.prepare( DBTable.Salinities.table)
            for item in items {
                retArray.append(DBSalinities(id: item[Expressions.id], stationID: item[Expressions.stationID], level: item[Expressions.level], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func numberOfEntries(stationID: Int, db: Connection) throws -> Int {
        let query =  DBTable.Salinities.table.filter(Expressions.stationID == Int64(stationID)).count
        do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }


    public static func numberOfEntries(db: Connection) throws -> Int {
        do {
            let count = try db.scalar( DBTable.Salinities.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

}
