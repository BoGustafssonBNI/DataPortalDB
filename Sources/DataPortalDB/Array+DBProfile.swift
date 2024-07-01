//
//  Array+DBProfile.swift
//
//
//  Created by Bo Gustafsson on 2024-06-28.
//

import Foundation

extension Array where Element == DBProfile {
    public func toAverage(for timeAverageType: DBProfile.TimeAverageTypes) -> [[DBProfile]] {
        var result = [[DBProfile]]()
        if let firstProfile = self.first, let lastProfile = self.last {
            switch timeAverageType {
            case .Daily(let interval):
                var pTemp = [firstProfile]
                for i in 1..<self.count {
                    if pTemp.first!.dayNo <= self[i].dayNo + interval - 1 {
                        pTemp.append(self[i])
                    } else {
                        result.append(pTemp)
                        pTemp = [self[i]]
                    }
                }
            case .Monthly(let interval):
                var pTemp = [firstProfile]
                var oldMonthNumber = (firstProfile.year * 12 + firstProfile.month - 1)/interval
                for i in 1..<self.count {
                    let newMonthNumber = (self[i].year * 12 + self[i].month - 1)/interval
                    if oldMonthNumber == newMonthNumber {
                        pTemp.append(self[i])
                    } else {
                        result.append(pTemp)
                        pTemp = [self[i]]
                        oldMonthNumber = newMonthNumber
                    }
                }
            case .Annual(let interval):
                var pTemp = [firstProfile]
                for i in 1..<self.count {
                    if pTemp.first!.year <= self[i].year + interval - 1 {
                        pTemp.append(self[i])
                    } else {
                        result.append(pTemp)
                        pTemp = [self[i]]
                    }
                }
            case .Winter(let startMonth, let endMonth), .Summer(let startMonth, let endMonth):
                let firstYear = firstProfile.year
                let lastYear = lastProfile.year
                for year in firstYear...lastYear {
                    let start : DateComponents
                    if startMonth > endMonth {
                        start = DateComponents.init(calendar: Calendar.UTCCalendar, year: year-1, month: startMonth, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)
                    } else {
                        start = DateComponents.init(calendar: Calendar.UTCCalendar, year: year, month: startMonth, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)
                    }
                    let firstDate = Calendar.UTCCalendar.date(from: start)!
                    let end : DateComponents
                    if endMonth < 12 {
                       end = DateComponents.init(calendar: Calendar.UTCCalendar, year: year, month: endMonth+1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)
                    } else {
                        end = DateComponents.init(calendar: Calendar.UTCCalendar, year: year+1, month: 1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 0)
                    }
                    let endDate = Calendar.UTCCalendar.date(from: end)!
                    let pTemp = self.filter({$0.date >= firstDate && $0.date < endDate})
                    result.append(pTemp)
                }
             case .Seasonal:
                for month in 1...12 {
                    let pTemp = self.filter({$0.month == month})
                    result.append(pTemp)
                }
            case .All:
                return [self]
            }
        }
        return result
    }
}
