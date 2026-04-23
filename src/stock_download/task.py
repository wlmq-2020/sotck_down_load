#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""分层爬取任务封装"""
import os
from datetime import datetime

import pandas as pd
import pysnowball as xq
from tqdm import tqdm

from .client import XueqiuClient
from .config import DATA_SCOPE, DEFAULT_STOCKS, PATH, SCHEDULE
from .finance import FinanceFetcher
from .money_flow import MoneyFlowFetcher
from .quote import QuoteFetcher
from .utils import DataSaver

# 初始化客户端
client = XueqiuClient()
quote_fetcher = QuoteFetcher()
finance_fetcher = FinanceFetcher()
money_fetcher = MoneyFlowFetcher()
# 加载股票列表
STOCK_LIST_PATH = PATH["stock_list_path"]
if not os.path.exists(STOCK_LIST_PATH):
    # 如果没有股票列表，默认给个示例，用户可以自己替换成全量
    stock_list = DEFAULT_STOCKS
else:
    df = pd.read_csv(STOCK_LIST_PATH)
    stock_list = df['full_code'].tolist()

# 加载自选股票列表
CUSTOM_STOCK_LIST_PATH = "./data/自选股票列表.csv"
custom_stock_list = []
if os.path.exists(CUSTOM_STOCK_LIST_PATH):
    df_custom = pd.read_csv(CUSTOM_STOCK_LIST_PATH)
    custom_stock_list = df_custom['full_code'].tolist()
    # 合并到总股票列表，去重
    stock_list = list(set(stock_list + custom_stock_list))
def get_today_str():
    """获取今日日期字符串"""
    return datetime.now().strftime('%Y%m%d')
def get_week_str():
    """获取本周日期字符串"""
    return datetime.now().strftime('%Y%U')
def get_month_str():
    """获取本月日期字符串"""
    return datetime.now().strftime('%Y%m')
def update_stock_json(code, field_name, data, append=False, unique_key="交易日期", save_to_quote_dir=False):
    """更新指定股票的JSON文件，增量更新字段
    :param code: 股票代码，比如SZ000422
    :param field_name: 要更新的字段名，比如quote/money_flow/finance
    :param data: 要更新的数据
    :param append: 是否追加模式，用于历史数据补全，避免重复
    :param unique_key: 唯一键，用于去重，默认是交易日期
    :param save_to_quote_dir: 是否保存到quote目录，默认是，和K线数据放在一起
    """
    if save_to_quote_dir:
        json_dir = "./data/quote/"
        os.makedirs(json_dir, exist_ok=True)
        json_path = os.path.join(json_dir, f"{code}.json")
    else:
        json_path = os.path.join("./data/", f"{code}.json")
    mode = "append" if append else "cover"

    # 使用统一DataSaver保存数据
    DataSaver.save(data, json_path, format="json", mode=mode, field_name=field_name, unique_key=unique_key)

def batch_fetch(func, desc="批量拉取", json_field=None, append=False, unique_key="交易日期"):
    """通用批量拉取函数，仅更新JSON，无其他多余文件
    :param func: 单只股票拉取函数
    :param desc: 进度条描述
    :param json_field: 要更新到JSON的字段名，必传
    :param append: 是否增量追加模式，默认覆盖
    :param unique_key: 增量模式下的去重唯一键
    """
    failed = []
    for code in tqdm(stock_list, desc=desc):
        try:
            data = func(code)
            # 更新JSON
            update_stock_json(code, json_field, data, append=append, unique_key=unique_key)
        except Exception:
            failed.append(code)
            continue
    # 失败提示
    if failed:
        print(f"本次任务失败{len(failed)}只股票：{','.join(failed)}")
    else:
        print(f"{desc}完成，成功更新{len(stock_list)}只股票的JSON")
# ------------------- 日级任务 -------------------
def daily_task():
    """日级爬取任务：每日收盘后运行"""
    today = get_today_str()
    print(f"===== 开始执行{today}日级爬取任务 =====")
    # 1. 当日行情快照
    batch_fetch(quote_fetcher.get_single_quote,
                desc="更新当日行情",
                json_field='quote')
    # 2. 当日日线K线（统一格式，中文字段）
    batch_fetch(lambda code: quote_fetcher.get_history_kline(code, days=1),
                desc="更新当日K线",
                json_field='daily_kline')
    # 3. 增量更新历史K线（补全最近30天缺失的数据，增量追加）
    batch_fetch(lambda code: quote_fetcher.get_history_kline(code, days=DATA_SCOPE["daily_increment_kline_days"]),
                desc="增量更新历史K线",
                json_field='history_kline',
                append=True,
                unique_key="交易日期")
    # 3. 当日资金流向
    batch_fetch(money_fetcher.get_stock_money_flow,
                desc="更新当日资金流向",
                json_field='money_flow')
    # 4. 当日融资融券
    batch_fetch(lambda code: pd.DataFrame(xq.margin(code)['data']['items']),
                desc="更新融资融券数据",
                json_field='margin')
    # 5. 当日大宗交易
    batch_fetch(lambda code: pd.DataFrame(xq.blocktrans(code)['data']['items']),
                desc="更新大宗交易数据",
                json_field='block_trans')
    # 6. 当日股东户数变化
    batch_fetch(lambda code: pd.DataFrame(xq.holders(code)['data']['items']),
                desc="更新股东户数数据",
                json_field='holders')
    print(f"===== {today}日级任务执行完成 =====")
# ------------------- 周级任务 -------------------
def weekly_task():
    """周级爬取任务：每周日运行"""
    week = get_week_str()
    print(f"===== 开始执行第{week}周周级爬取任务 =====")
    # 1. 最新财报数据
    batch_fetch(finance_fetcher.get_finance_report,
                desc="更新财报数据",
                json_field='finance')
    # 2. 最新公司公告（最近10条）
    batch_fetch(lambda code: pd.DataFrame(xq.report(code)['data']['items'][:10]),
                desc="更新公司公告",
                json_field='announcement')
    # 3. 机构持仓变化
    batch_fetch(lambda code: pd.DataFrame(xq.org_holding_change(code)['data']['items']),
                desc="更新机构持仓数据",
                json_field='org_holding')
    # 4. 周K线更新
    batch_fetch(lambda code: quote_fetcher.get_history_kline(code, days=7),
                desc="更新周K线",
                json_field='weekly_kline')
    # 5. 全量补全历史K线（补全近5年所有缺失数据，每周执行一次）
    batch_fetch(lambda code: quote_fetcher.get_history_kline(code, days=DATA_SCOPE["weekly_full_kline_days"]),
                desc="全量补全历史K线",
                json_field='history_kline',
                append=True,
                unique_key="交易日期")
    print(f"===== 第{week}周周级任务执行完成 =====")
# ------------------- 月级任务 -------------------
def monthly_task():
    """月级爬取任务：每月1号运行"""
    month = get_month_str()
    print(f"===== 开始执行{month}月级爬取任务 =====")
    # 1. 公司基本信息
    batch_fetch(lambda code: xq.f10[code] if code in xq.f10 else {},
                desc="更新公司基本信息",
                json_field='company_info')
    # 2. 行业分类
    batch_fetch(lambda code: xq.industry(code)['data'],
                desc="更新行业分类数据",
                json_field='industry')
    # 3. 业务分析
    batch_fetch(lambda code: pd.DataFrame(xq.business_analysis(code)['data']['items']),
                desc="更新业务分析数据",
                json_field='business_analysis')
    # 4. 分红送转记录
    batch_fetch(lambda code: pd.DataFrame(xq.bonus(code)['data']['items']),
                desc="更新分红送转数据",
                json_field='bonus')
    # 5. 股本变动记录
    batch_fetch(lambda code: pd.DataFrame(xq.shareschg(code)['data']['items']),
                desc="更新股本变动数据",
                json_field='shares_change')
    print(f"===== {month}月级任务执行完成 =====")
# ------------------- 历史数据补全 -------------------
def fill_history_kline(days: int = DATA_SCOPE["default_kline_days"]):
    """补全股票历史日K数据，默认补全5年
    :param days: 补全的天数，默认1825天≈5年
    """
    print(f"===== 开始补全{len(stock_list)}只股票的{days}天历史K线数据 =====")

    def fetch_and_fill(code):
        # 先检查是否已有历史数据
        json_path = os.path.join("./data/", f"{code}.json")
        existing_days = 0
        if os.path.exists(json_path):
            import json
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and 'history_kline' in data:
                        existing_days = len(data['history_kline'])
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                # JSON文件损坏时，认为没有历史数据，重新补全
                print(f"警告：股票{code}的JSON文件损坏，将重新补全历史数据")
                existing_days = 0

        # 如果已有数据足够，跳过
        if existing_days >= days:
            print(f"股票{code}已有{existing_days}天历史数据，跳过")
            return pd.DataFrame()

        # 计算需要补全的天数
        need_days = days - existing_days
        print(f"股票{code}需要补全{need_days}天历史数据")
        df = quote_fetcher.get_history_kline(code, need_days)

        # 增量更新到JSON
        update_stock_json(code, 'history_kline', df, append=True, unique_key='交易日期')
        return df

    batch_fetch(fetch_and_fill, desc="补全历史K线数据", json_field=None)
    print("===== 历史K线数据补全完成 =====")

# ------------------- 调度服务 -------------------
def start_schedule():
    """启动定时调度服务"""
    import time

    import schedule
    # 配置任务
    schedule.every().day.at(SCHEDULE["daily_time"]).do(daily_task)
    schedule.every().sunday.at(SCHEDULE["weekly_time"]).do(weekly_task)
    schedule.every().month.at(SCHEDULE["monthly_time"]).do(monthly_task)
    print("===== 定时调度服务已启动 =====")
    print(f"每日{SCHEDULE['daily_time']}执行日级任务")
    print(f"每周日{SCHEDULE['weekly_time']}执行周级任务")
    print(f"每月1号{SCHEDULE['monthly_time']}执行月级任务")
    print("按Ctrl+C停止服务")
    # 保持运行
    while True:
        schedule.run_pending()
        time.sleep(60)

# ------------------- 股票筛选任务 -------------------
def filter_stocks():
    """筛选沪深主板非ST、市值50-300亿的股票，保存到股票列表.csv
    :return: 筛选后的股票列表，失败抛出异常
    """
    import os

    import pandas as pd
    import requests
    from dotenv import load_dotenv

    load_dotenv()
    cookie = os.getenv("XUEQIU_COOKIE")
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://xueqiu.com/hq/screener",
        "X-Requested-With": "XMLHttpRequest"
    }

    all_stocks = []
    page = 1
    size = 200

    # 分页拉取所有沪深A股
    while True:
        url = f"https://stock.xueqiu.com/v5/stock/screener/quote/list.json?page={page}&size={size}&order=desc&orderby=market_capital&market=CN&type=sh_sz&_=1712990000000"
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            stock_list = data.get('data', {}).get('list', [])
            if not stock_list:
                break
            all_stocks.extend(stock_list)
            if len(stock_list) < size:
                break
            page += 1
        except Exception as e:
            raise RuntimeError(f"拉取第{page}页失败：{str(e)}")

    # 筛选条件
    filtered = []
    for stock in all_stocks:
        symbol = stock['symbol']
        name = stock['name']
        # 1. 排除ST/*ST股票
        if 'ST' in name or '*ST' in name:
            continue
        # 2. 保留上证主板(SH60开头)、深证主板(SZ00开头)、创业板(SZ30开头)，排除科创板、北交所
        if not (symbol.startswith('SH60') or symbol.startswith('SZ00') or symbol.startswith('SZ30')):
            continue
        # 3. 总市值在50-300亿之间（接口返回单位为元，转成亿）
        market_cap = stock.get('market_capital', 0) / 100000000
        if not (50 <= market_cap <= 300):
            continue
        # 格式化写入格式
        code = symbol[2:]  # 去掉前缀，比如SZ000422 -> 000422
        filtered.append({
            'code': code,
            'name': name,
            'full_code': symbol
        })

    if len(filtered) == 0:
        raise RuntimeError("没有找到符合条件的股票")

    # 写入股票列表.csv
    df = pd.DataFrame(filtered)
    output_path = './data/股票列表.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

    return filtered, output_path

def update_custom_stocks():
    """一键更新所有自选股票的全量数据，不需要任何参数
    自动实现所有更新逻辑：实时行情、最近5年历史K线（智能增量）、财务报表、今日资金流向
    所有数据自动按唯一键去重，不会重复下载已有数据
    """
    if not custom_stock_list:
        print("错误：未找到自选股票列表，请先在 ./data/自选股票列表.csv 中添加股票")
        return

    print(f"找到 {len(custom_stock_list)} 只自选股票，开始全量更新...")

    for code in tqdm(custom_stock_list, desc="更新自选股数据"):
        try:
            # 1. 更新实时行情
            quote_data = quote_fetcher.get_single_quote(code)
            update_stock_json(code, "quote", quote_data, append=False, save_to_quote_dir=True)

            # 2. 智能增量更新最近5年历史K线，自动去重，不会重复下载已有交易日数据
            df_kline = quote_fetcher.get_history_kline(code, days=DATA_SCOPE["default_kline_days"])
            update_stock_json(code, "history_kline", df_kline.to_dict(orient='records'), append=True, unique_key="交易日期", save_to_quote_dir=True)

            # 3. 更新财务报表
            df_finance = finance_fetcher.get_finance_report(code, report_type="all")
            update_stock_json(code, "finance", df_finance.to_dict(orient='records'), append=False, save_to_quote_dir=True)

            # 4. 更新今日资金流向
            df_money = money_fetcher.get_stock_money_flow(code)
            update_stock_json(code, "money_flow", df_money.to_dict(orient='records'), append=True, unique_key="时间", save_to_quote_dir=True)

        except Exception as e:
            print(f"警告：更新 {code} 失败：{str(e)}")
            continue

    print(f"\n✅ 所有自选股票全量更新完成！数据统一保存在 ./data/quote/ 目录下，每个股票对应一个JSON文件")
