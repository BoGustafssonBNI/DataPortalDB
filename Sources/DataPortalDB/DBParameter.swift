//
//  ParameterDB.swift
//  DataPortalSQlite
//
//  Created by Bo Gustafsson on 2018-04-06.
//  Copyright © 2018 Bo Gustafsson. All rights reserved.
//

import Foundation
import SQLite




struct DBParameter {
    var id = 0
    var name = ""
    struct TableDescription {
        static let id = "id"
        static let name = "name"
    }
    
    struct Expressions {
        static let id = Expression<Int64>(TableDescription.id)
        static let name = Expression<String>(TableDescription.name)
     }
    
    
    init(){}
    init(id: Int, name: String) {
        self.id = id
        self.name = name
    }
    init(id id64: Int64, name: String) {
        self.id = Int(id64)
        self.name = name
    }
    
    static func createTable(db: Connection) throws {
        do {
            try db.run(DBTable.Parameter.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.name)
            })
        } catch {
            throw DBError.TableCreateError
        }
    }
    static func deleteTable(db: Connection) throws {
        do {
            try db.run(DBTable.Parameter.table.delete())
        } catch {
            throw DBError.TableDeleteError
        }
    }

    
    mutating func insert(db: Connection) throws {
         let insertStatement = DBTable.Parameter.table.insert(Expressions.name <- name)
        do {
            let rowID = try db.run(insertStatement)
            id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    
    func delete(db: Connection) throws -> Void {
        let query = DBTable.Parameter.table.filter(Expressions.id == Int64(id))
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
        let expression = Expressions.name == name
        let query = DBTable.Parameter.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
//                    print("Mismatch between DB and model: parameter id = \(self.id) db id = \(id)")
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
    
    static func find(id: Int, db: Connection) throws -> DBParameter? {
        let query = DBTable.Parameter.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBParameter(id: item[Expressions.id], name: item[Expressions.name])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    
    static func find(name: String, db: Connection) throws -> DBParameter? {
        let query = DBTable.Parameter.table.filter(Expressions.name == name)
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBParameter(id: item[Expressions.id], name: item[Expressions.name])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }

    static func findAll(db: Connection) throws -> [DBParameter] {
        var retArray = [DBParameter]()
        do {
            let items = try db.prepare(DBTable.Parameter.table)
            for item in items {
                retArray.append(DBParameter(id: item[Expressions.id], name: item[Expressions.name]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    static func numberOfEntries(db: Connection) throws -> Int {
        do {
            let count = try db.scalar(DBTable.Parameter.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

    
}
