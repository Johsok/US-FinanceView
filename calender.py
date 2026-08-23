# -*- coding: utf-8 -*-
"""產出台股／美股平日休市日清單 holidays.json。"""
import json
from datetime import datetime

import pandas as pd
import pandas_market_calendars as mcal


def get_closed_weekdays(market_code, year_list):
    """
    找出平日但該市場沒開盤的日期。
    @param {string} market_code - XTAI 或 NYSE
    @param {Array<number>} year_list - 西元年清單
    @returns {Array<string>} YYYY-MM-DD 清單
    """
    cal = mcal.get_calendar(market_code)
    start_date = f"{min(year_list)}-01-01"
    end_date = f"{max(year_list)}-12-31"
    all_weekdays = pd.date_range(start=start_date, end=end_date, freq="B")
    schedule = cal.schedule(start_date=start_date, end_date=end_date)
    open_dates = schedule.index
    all_weekdays_set = set(all_weekdays.strftime("%Y-%m-%d"))
    open_dates_set = set(open_dates.strftime("%Y-%m-%d"))
    closed_weekdays = sorted(list(all_weekdays_set - open_dates_set))
    print(f" -> {market_code} 在 {year_list} 期間共有 {len(closed_weekdays)} 個平日休市日")
    return closed_weekdays


def main():
    """產生今年與明年的休市日 JSON。"""
    current_year = datetime.now().year
    target_years = [current_year, current_year + 1]
    print(f"正在分析年份: {target_years} ...")
    output_data = {
        "TW": get_closed_weekdays("XTAI", target_years),
        "US": get_closed_weekdays("NYSE", target_years),
    }
    filename = "holidays.json"
    with open(filename, "w", encoding="utf-8") as handle:
        json.dump(output_data, handle, indent=4)
    print(f"\n★ 成功！已儲存純日期清單至 {filename}")


if __name__ == "__main__":
    main()
