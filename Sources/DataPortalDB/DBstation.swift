//
//  DBstation.swift
//  DataPortalSQlite
//
//  Created by Bo Gustafsson on 2018-04-06.
//  Copyright © 2018 Bo Gustafsson. All rights reserved.
//

import Foundation
import SQLite




public struct DBstation {
    public var id = 0
    public var name = ""
    public var intLat = 0
    public var intLon = 0
    public var intDist = 0
    private func intPos2Dec(pos: Int) -> Double {
        let intDeg = pos/100
        let intMin = pos - intDeg * 100
        return Double(intDeg) + Double(intMin)/60.0
    }
    public var lat : Double {
        get {
            return intPos2Dec(pos: intLat)
        }
    }
    public var lon : Double {
        get {
            return intPos2Dec(pos: intLon)
        }
    }
    public var latMin : Double {
        get {
            return lat - Double(intDist)/60.0
        }
    }
    public var latMax : Double {
        get {
            return lat + Double(intDist)/60.0
        }
    }
    public var lonMin : Double {
        get {
            return lon - Double(intDist)/60.0
        }
    }
    public var lonMax : Double {
        get {
            return lon + Double(intDist)/60.0
        }
    }
    public struct TableDescription {
        public static let id = "id"
        public static let name = "name"
        public static let intLat = "latitude"
        public static let intLon = "longitude"
        public static let intDist = "distance"
    }
    
    struct Expressions {
        static let id = Expression<Int64>(TableDescription.id)
        static let name = Expression<String>(TableDescription.name)
        static let intLat = Expression<Int64>(TableDescription.intLat)
        static let intLon = Expression<Int64>(TableDescription.intLon)
        static let intDist = Expression<Int64>(TableDescription.intDist)
    }
    
    
    init(){}
    init(id: Int, name: String, intLat: Int, intLon: Int, intDist: Int) {
        self.id = id
        self.name = name
        self.intLat = intLat
        self.intLon = intLon
        self.intDist = intDist
     }
    init(id id64: Int64, name: String, intLat intLat64: Int64, intLon intLon64: Int64, intDist intDist64: Int64) {
        self.id = Int(id64)
        self.name = name
        self.intLat = Int(intLat64)
        self.intLon = Int(intLon64)
        self.intDist = Int(intDist64)
    }
    
    public static func createTable(db: Connection) throws {
         do {
            try db.run(DBTable.Stations.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.name)
                t.column(Expressions.intLat)
                t.column(Expressions.intLon)
                t.column(Expressions.intDist)
            })
        } catch {
            throw DBError.TableCreateError
        }
    }
    public static func deleteTable(db: Connection) throws {
          do {
            try db.run(DBTable.Stations.table.delete())
        } catch {
            print(error)
            throw DBError.TableDeleteError
        }
    }
    
    
    public mutating func insert(db: Connection) throws {
         let insertStatement = DBTable.Stations.table.insert(Expressions.name <- name,
                                                           Expressions.intLat <- Int64(intLat),
                                                           Expressions.intLon <- Int64(intLon),
                                                           Expressions.intDist <- Int64(intDist))
        do {
            let rowID = try db.run(insertStatement)
            id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    
    func delete (db: Connection) throws -> Void {
         let query = DBTable.Stations.table.filter(Expressions.id == Int64(id))
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
        let expression = Expressions.name == name && Expressions.intLat == Int64(intLat) && Expressions.intLon == Int64(intLon) && Expressions.intDist == Int64(intDist)
        let query = DBTable.Stations.table.select(distinct: Expressions.id).filter(expression)
        do {
            let items = try db.prepare(query)
            for item in items {
                let id = Int(item[Expressions.id])
                if id != self.id {
                    print("Mismatch between DB and model: Station id = \(self.id) db id = \(id)")
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
    
    public static func find(id: Int, db: Connection) throws -> DBstation? {
         let query = DBTable.Stations.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBstation(id: item[Expressions.id], name: item[Expressions.name], intLat: item[Expressions.intLat], intLon: item[Expressions.intLon], intDist: item[Expressions.intDist])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    
    public enum DataTypes {
        case intLat
        case intLon
        case intDist
    }
    
    public static func update(id: Int, for variable: DataTypes, value: Int, db: Connection) throws -> Int {
        let query = DBTable.Stations.table.filter(Expressions.id == Int64(id))
        
        do {
            let valueExpression : Expression<Int64>
            switch variable {
            case .intLat:
                valueExpression = Expressions.intLat
            case .intLon:
                valueExpression = Expressions.intLon
            case .intDist:
                valueExpression = Expressions.intDist
            }
            let result = try db.run(query.update(valueExpression <- Int64(value)))
            return result
        } catch {
            throw DBError.UpdateError
        }
    }
    public static func update(id: Int, name: String, db: Connection) throws -> Int {
        let query = DBTable.Stations.table.filter(Expressions.id == Int64(id))
        
        do {
            let result = try db.run(query.update(Expressions.name <- name))
            return result
        } catch {
            throw DBError.UpdateError
        }
    }
    
    public static func numberOfEntries(db: Connection) throws -> Int {
        do {
            let count = try db.scalar(DBTable.Stations.table.count)
            return count
        } catch {
            throw DBError.SearchError
        }
    }

    public static func findAll(db: Connection) throws -> [DBstation] {
         var retArray = [DBstation]()
        do {
            let items = try db.prepare(DBTable.Stations.table)
            for item in items {
                retArray.append(DBstation(id: item[Expressions.id], name: item[Expressions.name], intLat: item[Expressions.intLat], intLon: item[Expressions.intLon], intDist: item[Expressions.intDist]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    
    
}
