//
//  DBValidationData.swift
//  DataPortalDB
//
//  Created by Bo Gustafsson on 2024-10-24.
//


import Foundation
import SQLite


public struct DBValidationData: Comparable, Equatable, Hashable {
    public static func < (lhs: DBValidationData, rhs: DBValidationData) -> Bool {
        return lhs.meanValue < rhs.meanValue
    }
    public static func == (lhs: DBValidationData, rhs: DBValidationData) -> Bool {
        return lhs.meanValue == rhs.meanValue
    }
    
    public var id = 0
    public var stationID = 0
    public var profileID = 0
    public var depthID = 0
    public var parameterID = 0
    public var meanValue = 0.0
    public var medianValue = 0.0
    public var standardError = 0.0
    public var lowerConfidence = 0.0
    public var upperConfidence = 0.0
    public var numberInEstimate = 0
    public var numberOfObservations = 0

    public struct TableDescription {
        public static let id = "id"
        public static let stationID = "stationID"
        public static let profileID = "profileID"
        public static let depthID = "depthID"
        public static let parameterID = "parameterID"
        public static let meanValue = "meanValue"
        public static let medianValue = "medianValue"
        public static let standardError = "standardError"
        public static let lowerConfidence = "lowerConfidence"
        public static let upperConfidence = "upperConfidence"
        public static let numberInEstimate = "numberInEstimate"
        public static let numberOfObservations = "numberOfObservations"
    }
    
    struct Expressions {
        static let id = SQLite.Expression<Int64>(TableDescription.id)
        static let stationID = SQLite.Expression<Int64>(TableDescription.stationID)
        static let profileID = SQLite.Expression<Int64>(TableDescription.profileID)
        static let depthID = SQLite.Expression<Int64>(TableDescription.depthID)
        static let parameterID = SQLite.Expression<Int64>(TableDescription.parameterID)
        static let meanValue = SQLite.Expression<Double>(TableDescription.meanValue)
        static let medianValue = SQLite.Expression<Double>(TableDescription.medianValue)
        static let standardError = SQLite.Expression<Double>(TableDescription.standardError)
        static let lowerConfidence = SQLite.Expression<Double>(TableDescription.lowerConfidence)
        static let upperConfidence = SQLite.Expression<Double>(TableDescription.upperConfidence)
        static let numberInEstimate = SQLite.Expression<Int64>(TableDescription.numberInEstimate)
        static let numberOfObservations = SQLite.Expression<Int64>(TableDescription.numberOfObservations)
    }
    
    public init(){}
    public init(id: Int, stationID: Int, profileID: Int, depthID: Int, parameterID: Int, meanValue: Double, medianValue: Double, standardError: Double, lowerConfidence: Double, upperConfidence: Double, numberInEstimate: Int, numberOfObservations: Int) {
        self.id = id
        self.stationID = stationID
        self.profileID = profileID
        self.depthID = depthID
        self.parameterID = parameterID
        self.meanValue = meanValue
        self.medianValue = medianValue
        self.standardError = standardError
        self.lowerConfidence = lowerConfidence
        self.upperConfidence = upperConfidence
        self.numberInEstimate = numberInEstimate
        self.numberOfObservations = numberOfObservations
    }
    init(id id64: Int64, stationID stationID64: Int64, profileID profileID64: Int64, depthID depthID64: Int64, parameterID parameterID64: Int64, meanValue: Double, medianValue: Double, standardError: Double, lowerConfidence: Double, upperConfidence: Double, numberInEstimate numberInEstimate64: Int64, numberOfObservations numberOfObservations64: Int64) {
        self.id = Int(id64)
        self.stationID = Int(stationID64)
        self.parameterID = Int(parameterID64)
        self.profileID = Int(profileID64)
        self.depthID = Int(depthID64)
        self.meanValue = meanValue
        self.medianValue = medianValue
        self.standardError = standardError
        self.lowerConfidence = lowerConfidence
        self.upperConfidence = upperConfidence
        self.numberInEstimate = Int(numberInEstimate64)
        self.numberOfObservations = Int(numberOfObservations64)
    }
    
    public static func createTable(dbTable: DBTable, db: Connection) throws {
         do {
            try db.run(dbTable.table.create(ifNotExists: true) {t in
                t.column(Expressions.id, primaryKey: true)
                t.column(Expressions.stationID)
                t.column(Expressions.profileID)
                t.column(Expressions.depthID)
                t.column(Expressions.parameterID)
                t.column(Expressions.meanValue)
                t.column(Expressions.medianValue)
                t.column(Expressions.standardError)
                t.column(Expressions.lowerConfidence)
                t.column(Expressions.upperConfidence)
                t.column(Expressions.numberInEstimate)
                t.column(Expressions.numberOfObservations)
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
                                                   Expressions.meanValue <- meanValue,
                                                   Expressions.medianValue <- medianValue,
                                                   Expressions.standardError <- standardError,
                                                   Expressions.lowerConfidence <- lowerConfidence,
                                                   Expressions.upperConfidence <- upperConfidence,
                                                   Expressions.numberInEstimate <- Int64(numberInEstimate),
                                                   Expressions.numberOfObservations <- Int64(numberOfObservations))
        do {
            let rowID = try db.run(insertStatement)
            id = Int(rowID)
        } catch {
            throw DBError.InsertError
        }
    }
    public mutating func insert(db: Connection, insertStatement: Statement) throws {
        do {
            _ = try insertStatement.run(Int64(stationID), Int64(profileID), Int64(depthID), Int64(parameterID), meanValue, medianValue, standardError, lowerConfidence, upperConfidence, Int64(numberInEstimate), Int64(numberOfObservations))
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
    
    public static func delete(dbProfileID: Int, dbTable: DBTable, db: Connection) throws {
        let query = dbTable.table.filter(Expressions.profileID == Int64(dbProfileID))
        do {
            _ = try db.run(query.delete())
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

    public static func update(id: Int, meanValue: Double, medianValue: Double, standardError: Double, lowerConfidence: Double, upperConfidence: Double, numberInEstimate: Int, numberOfObservations: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            let result = try db.run(query.update(Expressions.meanValue <- meanValue,
                                                 Expressions.medianValue <- medianValue,
                                                 Expressions.standardError <- standardError,
                                                 Expressions.lowerConfidence <- lowerConfidence,
                                                 Expressions.upperConfidence <- upperConfidence,
                                                 Expressions.numberInEstimate <- Int64(numberInEstimate),
                                                 Expressions.numberOfObservations <- Int64(numberOfObservations)))
            return result
        } catch {
            throw DBError.UpdateError
        }
    }
    
    public static func find(id: Int, dbTable: DBTable, db: Connection) throws -> DBValidationData? {
        let query = dbTable.table.filter(Expressions.id == Int64(id))
        do {
            let items = try db.prepare(query)
            for item in  items {
                return DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations])
            }
        } catch {
            throw DBError.SearchError
        }
        return nil
    }
    public static func find(profileID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID))
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
    public static func find(profileID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(profileID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
        let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.parameterID == Int64(parameterID)).order(Expressions.depthID.asc)
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(profileID: Int, parameterID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
         let query = dbTable.table.filter(Expressions.profileID == Int64(profileID) && Expressions.parameterID == Int64(parameterID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
// Station based search
    public static func find(stationID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(stationID: Int, parameterID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.parameterID == Int64(parameterID)).order(Expressions.depthID.asc)
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }

    public static func find(stationID: Int, parameterID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
         let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.parameterID == Int64(parameterID) && Expressions.depthID == Int64(depthID))
        var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
            }
        } catch {
            throw DBError.SearchError
        }
        
        return retArray
    }
// Find all
    public static func findAll(dbTable: DBTable, db: Connection) throws -> [DBValidationData] {
         var retArray = [DBValidationData]()
        do {
            let items = try db.prepare(dbTable.table)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
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
        command = "SELECT max(\(TableDescription.meanValue)) FROM \(dbTable.tableName) WHERE (\(TableDescription.profileID) = ?) AND (\(TableDescription.parameterID) = ?)"
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
        command = "SELECT min(\(TableDescription.meanValue)) FROM \(dbTable.tableName) WHERE (\(TableDescription.profileID) = ?) AND (\(TableDescription.parameterID) = ?)"
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
    
    public static func find(profileIDRange: Range<Int>, parameterID: Int, at meanValue: Double, with tolerance: Double, from dbTable: DBTable, and db: Connection) throws -> [DBValidationData] {
        var retArray = [DBValidationData]()
        let query = dbTable.table.filter(Expressions.profileID >= Int64(profileIDRange.lowerBound) && Expressions.profileID <= Int64(profileIDRange.upperBound) && Expressions.parameterID == Int64(parameterID) && Expressions.meanValue >= meanValue - tolerance && Expressions.meanValue <= meanValue + tolerance)
        do {
            let items = try db.prepare(query)
            for item in items {
                retArray.append(DBValidationData(id: item[Expressions.id], stationID: item[Expressions.stationID], profileID: item[Expressions.profileID], depthID: item[Expressions.depthID], parameterID: item[Expressions.parameterID], meanValue: item[Expressions.meanValue], medianValue: item[Expressions.medianValue], standardError: item[Expressions.standardError], lowerConfidence: item[Expressions.lowerConfidence], upperConfidence: item[Expressions.upperConfidence], numberInEstimate: item[Expressions.numberInEstimate], numberOfObservations: item[Expressions.numberOfObservations]))
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
                result.append(item[Expressions.meanValue])
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

    
    public static func numberOfEntries(stationID: Int, parameterID: Int, depthID: Int, dbTable: DBTable, db: Connection) throws -> Int {
        let query = dbTable.table.filter(Expressions.stationID == Int64(stationID) && Expressions.parameterID == Int64(parameterID) && Expressions.depthID == Int64(depthID)).count
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
