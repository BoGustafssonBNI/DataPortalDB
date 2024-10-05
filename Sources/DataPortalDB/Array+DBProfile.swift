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
                    if self[i].dayNo <= pTemp.first!.dayNo  + interval - 1 {
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
                    if self[i].year <= pTemp.first!.year + interval - 1 {
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
    public func toDailyAverage(interval: Int = 1) -> [[DBProfile]] {
        var result = [[DBProfile]]()
        if let firstProfile = self.first {
            var pTemp = [firstProfile]
            for i in 1..<self.count {
                if self[i].dayNo <= pTemp.first!.dayNo  + interval - 1 {
                    pTemp.append(self[i])
                } else {
                    result.append(pTemp)
                    pTemp = [self[i]]
                }
            }
        }
        return result
    }
    public func toMonthlyAverage(interval: Int = 1, dailyAverageProfiles: [[DBProfile]]? = nil) -> [[[DBProfile]]] {
        var result = [[[DBProfile]]]()
        if !self.isEmpty {
            var dProfiles: [[DBProfile]]
            if let dailyAverageProfiles = dailyAverageProfiles {
                dProfiles = dailyAverageProfiles
            } else {
                dProfiles = self.toDailyAverage()
            }
            if let firstProfile = dProfiles.first {
                var pTemp = [firstProfile]
                var oldMonthNumber = (firstProfile.first!.year * 12 + firstProfile.first!.month - 1)/interval
                for i in 1..<dProfiles.count {
                    let newMonthNumber = (dProfiles[i].first!.year * 12 + dProfiles[i].first!.month - 1)/interval
                    if oldMonthNumber == newMonthNumber {
                        pTemp.append(dProfiles[i])
                    } else {
                        result.append(pTemp)
                        pTemp = [dProfiles[i]]
                        oldMonthNumber = newMonthNumber
                    }
                }
            }
        }
        return result
    }
    public func toAnnualAverage(for timeAverageType: DBProfile.TimeAverageTypes, dailyAverageProfiles: [[DBProfile]]? = nil, monthlyAverageProfiles: [[[DBProfile]]]? = nil) -> [[[[DBProfile]]]] {
        var result = [[[[DBProfile]]]]()
        if !self.isEmpty {
            var mProfiles: [[[DBProfile]]]
            if let monthlyAverageProfiles = monthlyAverageProfiles {
                mProfiles = monthlyAverageProfiles
            } else {
                if let dailyAverageProfiles = dailyAverageProfiles {
                    mProfiles = self.toMonthlyAverage(dailyAverageProfiles: dailyAverageProfiles)
                } else {
                    mProfiles = self.toMonthlyAverage()
                }
            }
            if let firstProfile = mProfiles.first, let lastProfile = mProfiles.last {
                switch timeAverageType {
                case .Annual(interval: let interval):
                    var pTemp = [firstProfile]
                    for i in 1..<mProfiles.count {
                        if mProfiles[i].first!.first!.year <= pTemp.first!.first!.first!.year + interval - 1 {
                            pTemp.append(mProfiles[i])
                        } else {
                            result.append(pTemp)
                            pTemp = [mProfiles[i]]
                        }
                    }
                case .Winter(let startMonth, let endMonth), .Summer(let startMonth, let endMonth):
                    let firstYear = firstProfile.first!.first!.year
                    let lastYear = lastProfile.first!.first!.year
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
                        let pTemp = mProfiles.filter({$0.first!.first!.date >= firstDate && $0.first!.first!.date < endDate})
                        result.append(pTemp)
                    }
                case .Seasonal:
                    for month in 1...12 {
                        let pTemp = mProfiles.filter({$0.first!.first!.month == month})
                        result.append(pTemp)
                    }
                default:
                    break
                }
            }
        }
        return result
    }

}
