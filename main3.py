# -*- coding: utf-8 -*-
"""抓取今日起第 14 至第 20 天的台股財報／法說會資訊（MOPS 日曆＋官方損益表）。"""
from tw_scraper import run_scrape

SEARCH_DAYS = 7
START_OFFSET = 14


def main():
    """執行 main3 區間爬蟲。"""
    run_scrape(start_offset=START_OFFSET, search_days=SEARCH_DAYS)


if __name__ == "__main__":
    main()
