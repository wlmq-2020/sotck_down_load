#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""分层爬取任务封装"""
import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import pysnowball as xq
from .client import XueqiuClient
from .quote import QuoteFetcher
from .finance import FinanceFetcher
from .money_flow import MoneyFlowFetcher
# 初始化客户端
client = XueqiuClient()
quote_fetcher = QuoteFetcher()
finance_fetcher = FinanceFetcher()
money_fetcher = MoneyFlowFetcher()
# 加载股票列表
STOCK_LIST_PATH = './data/股票列表.csv'
if not os.path.exists(STOCK_LIST_PATH):
    # 如果没有股票列表，默认给个示例，用户可以自己替换成全量
    sample_stocks = ['SZ000001', 'SH600000', 'SH601318', 'SZ002594', 'SH600519']
    stock_list = sample_stocks
else:
    df = pd.read_csv(STOCK_LIST_PATH)
    stock_list = df['full_code'].tolist()
def get_today_str():
    """获取今日日期字符串"""
    return datetime.now().strftime('%Y%m%d')
def get_week_str():
    """获取本周日期字符串"""
    return datetime.now().strftime('%Y%U')
def get_month_str():
    """获取本月日期字符串"""
    return datetime.now().strftime('%Y%m')
def update_stock_json(code, field_name, data):
    """更新指定股票的JSON文件，增量更新字段，直接保存在data根目录
    :param code: 股票代码，比如SZ000422
    :param field_name: 要更新的字段名，比如quote/money_flow/finance
    :param data: 要更新的数据
    """
    import json
    json_path = os.path.join("./data/", f"{code}.json")
    # 读取已有JSON，不存在就新建
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            stock_data = json.load(f)
    else:
        stock_data = {"股票代码": code}
    # 更新字段
    if isinstance(data, pd.DataFrame):
        stock_data[field_name] = data.to_dict(orient='records')
    else:
        stock_data[field_name] = data
    # 重新计算启动信号（如果所有必要字段都有）
    if 'quote' in stock_data and 'finance' in stock_data and 'money_flow' in stock_data:
        try:
            signal = {}
            quote = stock_data['quote']
            finance = stock_data['finance']
            money_flow = stock_data['money_flow']
            # 量价信号
            signal['当日量比'] = round(quote['成交量(手)'] / (quote.get('5日均量', quote['成交量(手)'])), 2)
            signal['近5日涨跌幅(%)'] = quote.get('近5日涨跌幅', 0)
            signal['近20日涨跌幅(%)'] = quote.get('近20日涨跌幅', 0)
            signal['当日振幅(%)'] = quote['振幅(%)']
            # 资金信号
            total_flow = sum([item['资金流向(万元)'] for item in money_flow if item['时间'] != '今日汇总'])
            signal['当日主力资金净流入(万元)'] = round(total_flow, 2)
            # 基本面信号
            if len(finance) > 0:
                latest_finance = finance[0]
                signal['最新季度净利润同比(%)'] = latest_finance['净利润同比(%)']
                signal['动态市盈率(TTM)'] = quote.get('市盈率(TTM)', 0)
            # 计算启动分
            score = 0
            if signal['当日量比'] > 1.5: score += 2
            if signal['当日主力资金净流入(万元)'] > 2000: score += 2
            if signal.get('最新季度净利润同比(%)', 0) > 20: score += 2
            if signal['近5日涨跌幅(%)'] > 5 and signal['近5日涨跌幅(%)'] < 20: score += 2
            if signal.get('动态市盈率(TTM)', 0) > 0 and signal.get('动态市盈率(TTM)', 0) < 30: score += 1
            if quote['换手率(%)'] > 3 and quote['换手率(%)'] < 15: score +=1
            signal['启动概率分(0-10)'] = min(score, 10)
            stock_data['start_signal'] = signal
        except:
            pass
    # 保存更新后的JSON
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(stock_data, f, ensure_ascii=False, indent=2)

def batch_fetch(func, desc="批量拉取", json_field=None):
    """通用批量拉取函数，仅更新JSON，无其他多余文件
    :param func: 单只股票拉取函数
    :param desc: 进度条描述
    :param json_field: 要更新到JSON的字段名，必传
    """
    failed = []
    for code in tqdm(stock_list, desc=desc):
        try:
            data = func(code)
            # 更新JSON
            update_stock_json(code, json_field, data)
        except Exception as e:
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
    # 2. 当日日线K线
    batch_fetch(lambda code: pd.DataFrame(xq.kline(code, 'day', 1)['data']['item']),
                desc="更新当日K线",
                json_field='daily_kline')
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
    batch_fetch(lambda code: pd.DataFrame(xq.kline(code, 'week', 1)['data']['item']),
                desc="更新周K线",
                json_field='weekly_kline')
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
# ------------------- 调度服务 -------------------
def start_schedule():
    """启动定时调度服务"""
    import schedule
    import time
    # 配置任务
    schedule.every().day.at("17:00").do(daily_task)
    schedule.every().sunday.at("02:00").do(weekly_task)
    schedule.every().month.at("03:00").do(monthly_task)
    print("===== 定时调度服务已启动 =====")
    print("每日17:00执行日级任务")
    print("每周日02:00执行周级任务")
    print("每月1号03:00执行月级任务")
    print("按Ctrl+C停止服务")
    # 保持运行
    while True:
        schedule.run_pending()
        time.sleep(60)
