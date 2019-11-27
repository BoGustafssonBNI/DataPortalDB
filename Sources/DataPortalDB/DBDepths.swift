//
//  DBDepths.swift
//  SimpleDataSQL
//
//  Created by Bo Gustafsson on 2019-11-23.
//  Copyright © 2019 Bo Gustafsson. All rights reserved.
//

import Foundation
import SQLite


struct DBDepths {
    var id = 0
    var stationID = 0
    var depthID = 0
    var value = 0.0
    struct TableDescription {
        static let id = "id"
        static let stationID = "stationID"
        static let depthID = "depthID"
        static let value = "value"
    }
    
    struct Expressions {
        static let id = Expression<Int64>(TableDescription.id)
        static let stationID = Expression<Int64>(TableDescription.stationID)
        static let depthID = Expression<Int64>(TableDescription.depthID)
        static let value = Expression<Double>(TableDescription.value)
    }
    
    init(){}
    init(id: Int, stationID: Int, depthID: Int, value: Double) {
        self.id = id
        self.stationID = stationID
        self.depthID = depthID
        self.value = value
    }
    init(id id64: Int64, stationID stationID64: Int64, depthID depthID64: Int64, value: Double) {
        self.id = Int(id64)
        self.stationID = Int(stationID64)
        self.depthID = Int(depthID64)
        self.value = value
    }
    
    static func createTable(db: Connection) throws {
         do {
            try db.run(DBTable.Depths.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.stationID)
                t.column(Expressions.depthID)
                t.column(Expressions.value)
            })
        } catch {
            throw DBError.TableCreateError
        }
    }
    static func deleteTable(db: Connection) throws {
        do {
            try db.run(DBTable.Depths.table.delete())
        } catch {
            throw DBError.TableDeleteError
        }
    }

    static func createIndex(dbTable: DBTable, db: Connection) throws {
         do {
            try db.run(dbTable.table.createIndex(Expressions.stationID, ifNotExists: true))
        } catch {
            throw DBError.CouldNotCreateIndexError
        }
    }

    
    mutating func insert(db: Connection) throws {
        let insertStatement = DBTable.Depths.table.insert(Expressions.stationID <- Int64(stationID),
                                                          Expressions.depthID <- Int64(depthID),
                                                          Expressions.value <- value)
        do {
            let rowID = try db.run(insertStatement)
            id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    mutating func insert(db: Connection, insertStatement: Statement) throws {
        do {
            let rowID = try insertStatement.run(Int64(stationID), Int64(depthID), value)
            //                id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    

    func delete (db: Connection) throws -> Void {
        let query =  DBTable.Depths.table.filter(Expressions.id == Int64(id))
        do {
            let tmp = try db.run(query.delete())
            guard tmp == 1 else {
                throw DBError.DeleteError
            }
        } catch _ {
            throw DBError.DeleteError
        }
    }
    
    mutating func existOrInsert(db: Connection) throws {
        let expression = Expressions.stationID == Int64(stationID) && Expressions.depthID == Int64(depthID)
        let query =  DBTable.Depths.table.select(distinct: Expressions.id).filter(expression)
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
    mutating func existOrInsert(db: Connection, insertStatement: Statement) throws {
        let expression = Expressions.stationID == Int64(stationID) && Expressions.depthID == Int64(depthID)
        let query =  DBTable.Depths.table.select(distinct: Expressions.id).filter(expression)
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
    
    static func update(id: Int, value: Double, db: Connection) throws -> Int {
        let query =  DBTable.Depths.table.filter(Expressions.id == Int64(id))
        do {
            let result = try db.run(query.update(Expressions.value <- value))
            return result
        } catch {
            throw DBError.UpdateError
        }
    }

    static func find(id: Int, db: Connection) throws -> DBDepths? {
        let query =  DBTable.Depths.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBDepths(id: item[Expressions.id], stationID: item[Expressions.stationID], depthID: item[Expressions.depthID], value: item[Expressions.value])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    static func find(stationID: Int, db: Connection) throws -> [DBDepths] {
         let query =  DBTable.Depths.table.filter(Expressions.stationID == Int64(stationID))
        var retArray = [DBDepths]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBDepths(id: item[Expressions.id], stationID: item[Expressions.stationID], depthID: item[Expressions.depthID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    static func find(stationID: Int, depthID: Int, db: Connection) throws -> [DBDepths] {
        let query =  DBTable.Depths.table.filter(Expressions.stationID == Int64(stationID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBDepths]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBDepths(id: item[Expressions.id], stationID: item[Expressions.stationID], depthID: item[Expressions.depthID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }


    static func findAll(db: Connection) throws -> [DBDepths] {
         var retArray = [DBDepths]()
        do {
            let items = try db.prepare( DBTable.Depths.table)
            for item in items {
                retArray.append(DBDepths(id: item[Expressions.id], stationID: item[Expressions.stationID], depthID: item[Expressions.depthID], value: item[Expressions.value]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    static func numberOfEntries(stationID: Int, db: Connection) throws -> Int {
        let query =  DBTable.Depths.table.filter(Expressions.stationID == Int64(stationID)).count
        do {
            let count = try db.scalar(query)
            return count
        } catch {
            throw DBError.SearchError
        }
    }


    static func numberOfEntries(db: Connection) throws -> Int {
        do {
            let count = try db.scalar( DBTable.Depths.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

}
