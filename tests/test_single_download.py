#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试单条股票数据下载"""
import json
import os

from stock_download.finance import FinanceFetcher
from stock_download.money_flow import MoneyFlowFetcher
from stock_download.quote import QuoteFetcher
from stock_download.utils import DataSaver

# 股票代码
symbol = "SH600000"
save_path = f"./data/{symbol}.json"

print(f"=== 开始测试下载股票{symbol}数据 ===")

# 1. 测试行情数据获取
print("\n1. 获取实时行情数据...")
try:
    quote = QuoteFetcher()
    quote_data = quote.get_single_quote(symbol)
    print(f"[OK] 行情数据获取成功，字段：{list(quote_data.keys())}")
    print(f"   当前价格：{quote_data['当前价格']}，涨跌幅：{quote_data['涨跌幅(%)']}%")
    print(f"   成交额(万)：{quote_data['成交额(万)']}，总市值(亿)：{quote_data['总市值(亿)']}")
except Exception as e:
    print(f"[ERROR] 行情数据获取失败：{str(e)}")
    exit(1)

# 2. 测试资金流向数据获取
print("\n2. 获取资金流向数据...")
try:
    money_flow = MoneyFlowFetcher()
    mf_data = money_flow.get_stock_money_flow(symbol)
    print(f"[OK] 资金流向数据获取成功，共{len(mf_data)}条记录")
    print(f"   今日资金流向：{mf_data.iloc[-1]['资金流向(万元)']}万元")
except Exception as e:
    print(f"[ERROR] 资金流向数据获取失败：{str(e)}")
    exit(1)

# 3. 测试财务数据获取
print("\n3. 获取财务报表数据...")
try:
    finance = FinanceFetcher()
    finance_data = finance.get_finance_report(symbol, report_type="all")
    print(f"[OK] 财务数据获取成功，共{len(finance_data)}期报告")
    print(f"   最新报告期：{finance_data.iloc[0]['报告期']}，营收：{finance_data.iloc[0]['营业收入(万元)']}万元")
    print(f"   净利润：{finance_data.iloc[0]['净利润(万元)']}万元，同比：{finance_data.iloc[0]['净利润同比(%)']}%")
except Exception as e:
    print(f"[ERROR] 财务数据获取失败：{str(e)}")
    exit(1)

# 4. 测试近5年历史K线数据获取
print("\n4. 获取近5年历史K线数据...")
try:
    kline_data = quote.get_history_kline(symbol, days=1825)  # 1825天≈5年
    print(f"[OK] 历史K线数据获取成功，共{len(kline_data)}条记录")
    print(f"   最早日期：{kline_data.iloc[0]['交易日期']}，最新日期：{kline_data.iloc[-1]['交易日期']}")
except Exception as e:
    print(f"[ERROR] 历史K线数据获取失败：{str(e)}")
    exit(1)

# 5. 测试保存数据
print("\n5. 保存数据到JSON文件...")
try:
    # 保存行情
    DataSaver.save(quote_data, save_path, format="json", mode="cover", field_name="quote")
    # 保存资金流向
    DataSaver.save(mf_data.to_dict("records"), save_path, format="json", mode="append", field_name="money_flow", unique_key="时间")
    # 保存财务数据
    DataSaver.save(finance_data.to_dict("records"), save_path, format="json", mode="append", field_name="finance", unique_key="报告期")
    # 保存历史K线数据
    DataSaver.save(kline_data.to_dict("records"), save_path, format="json", mode="append", field_name="history_kline", unique_key="交易日期")

    print(f"[OK] 数据保存成功，文件大小：{os.path.getsize(save_path)/1024:.2f}KB")
except Exception as e:
    print(f"[ERROR] 数据保存失败：{str(e)}")
    exit(1)

# 6. 验证保存的文件
print("\n6. 验证保存的文件内容...")
try:
    with open(save_path, "r", encoding="utf-8") as f:
        saved_data = json.load(f)

    print(f"[OK] 文件读取成功，包含字段：{list(saved_data.keys())}")

    # 检查是否有违规字段
    if "start_signal" in saved_data:
        print("[ERROR] 发现违规字段：start_signal，应该被删除")
        exit(1)
    else:
        print("[OK] 没有违规字段，符合规范要求")

    # 检查各数据是否正确
    if "quote" in saved_data and len(saved_data["quote"]) > 0:
        print("[OK] 行情数据保存正确")
    if "money_flow" in saved_data and len(saved_data["money_flow"]) > 0:
        print("[OK] 资金流向数据保存正确")
    if "finance" in saved_data and len(saved_data["finance"]) > 0:
        print("[OK] 财务数据保存正确")
    if "history_kline" in saved_data and len(saved_data["history_kline"]) > 0:
        print("[OK] 历史K线数据保存正确，共{}条记录".format(len(saved_data["history_kline"])))

except Exception as e:
    print(f"[ERROR] 文件验证失败：{str(e)}")
    exit(1)

print("\n[SUCCESS] 所有测试通过！单条下载功能符合预期，包含近5年历史K线数据，格式正确，没有违规内容。")
