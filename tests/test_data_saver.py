#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""DataSaver工具类单元测试用例"""
import json
import os

import pandas as pd

from src.stock_download.utils import DataSaver

# 测试用临时文件路径
TEST_JSON_PATH = "./data/test_SZ000001.json"
TEST_CSV_PATH = "./data/test_data.csv"
TEST_EXCEL_PATH = "./data/test_data.xlsx"

# 测试数据示例
TEST_HISTORY_KLINE = [
    {"交易日期": "2024-04-01", "开盘价": 10.0, "最高价": 10.5, "最低价": 9.8, "收盘价": 10.2, "成交量(手)": 10000, "成交额(万)": 1020.0},
    {"交易日期": "2024-04-02", "开盘价": 10.2, "最高价": 10.8, "最低价": 10.0, "收盘价": 10.5, "成交量(手)": 12000, "成交额(万)": 1260.0}
]

TEST_QUOTE = {
    "股票代码": "SZ000001", "当前价格": 10.5, "涨跌幅(%)": 2.94, "成交量(手)": 10000, "成交额(万)": 1050.0, "换手率(%)": 0.05
}

TEST_FINANCE = [
    {"报告期": "2023年年报", "净利润(万元)": 500000, "净利润同比(%)": 10.5, "毛利率(%)": 35.2},
    {"报告期": "2024年一季报", "净利润(万元)": 130000, "净利润同比(%)": 8.2, "毛利率(%)": 34.8}
]

TEST_MONEY_FLOW = [
    {"时间": "09:30", "主力净流入(万元)": 1000, "散户净流入(万元)": -500, "成交额(万)": 5000},
    {"时间": "09:35", "主力净流入(万元)": 800, "散户净流入(万元)": -300, "成交额(万)": 4500}
]


def setup_module():
    """测试前置：清理临时测试文件"""
    for path in [TEST_JSON_PATH, TEST_CSV_PATH, TEST_EXCEL_PATH]:
        if os.path.exists(path):
            os.remove(path)


def teardown_module():
    """测试后置：清理临时测试文件"""
    for path in [TEST_JSON_PATH, TEST_CSV_PATH, TEST_EXCEL_PATH]:
        if os.path.exists(path):
            os.remove(path)


class TestDataSaver:
    """DataSaver工具类测试套件"""

    def test_json_save_cover_mode(self):
        """测试JSON格式全量覆盖模式保存"""
        # 保存测试数据
        DataSaver.save(TEST_HISTORY_KLINE, TEST_JSON_PATH, format="json", mode="cover", field_name="history_kline")
        # 验证文件存在
        assert os.path.exists(TEST_JSON_PATH)
        # 验证内容正确
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "history_kline" in data
        assert len(data["history_kline"]) == 2
        assert data["history_kline"][0]["交易日期"] == "2024-04-01"
        assert data["history_kline"][0]["收盘价"] == 10.2

    def test_json_save_append_mode(self):
        """测试JSON格式增量追加模式保存，自动按唯一键去重"""
        # 已有两条数据，追加一条新的，一条重复的
        new_data = [
            {"交易日期": "2024-04-02", "开盘价": 10.2, "最高价": 10.8, "最低价": 10.0, "收盘价": 10.5, "成交量(手)": 12000, "成交额(万)": 1260.0},  # 重复
            {"交易日期": "2024-04-03", "开盘价": 10.5, "最高价": 11.0, "最低价": 10.2, "收盘价": 10.8, "成交量(手)": 15000, "成交额(万)": 1620.0}   # 新增
        ]
        DataSaver.save(new_data, TEST_JSON_PATH, format="json", mode="append", field_name="history_kline", unique_key="交易日期")
        # 验证总数据3条，没有重复
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert len(data["history_kline"]) == 3
        # 验证排序正确
        assert data["history_kline"][2]["交易日期"] == "2024-04-03"

    def test_json_compatible_with_existing_format(self):
        """测试和现有JSON文件格式100%兼容，不会破坏原有字段"""
        # 先写入包含其他字段的模拟现有文件
        existing_data = {
            "股票代码": "SZ000001",
            "quote": TEST_QUOTE,
            "finance": TEST_FINANCE,
            "money_flow": TEST_MONEY_FLOW
        }
        with open(TEST_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)

        # 追加历史K线数据
        DataSaver.save(TEST_HISTORY_KLINE, TEST_JSON_PATH, format="json", mode="append", field_name="history_kline", unique_key="交易日期")

        # 验证原有字段都还在，没有被破坏
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "quote" in data
        assert "finance" in data
        assert "money_flow" in data
        assert "history_kline" in data
        assert data["quote"]["当前价格"] == 10.5
        assert len(data["finance"]) == 2

    def test_csv_save_and_load(self):
        """测试CSV格式保存和读取"""
        # 保存测试数据
        df = pd.DataFrame(TEST_HISTORY_KLINE)
        DataSaver.save(df, TEST_CSV_PATH, format="csv")
        # 验证文件存在
        assert os.path.exists(TEST_CSV_PATH)
        # 验证内容正确
        load_df = DataSaver.load(TEST_CSV_PATH, format="csv")
        assert len(load_df) == 2
        assert load_df.iloc[0]["交易日期"] == "2024-04-01"
        assert load_df.iloc[0]["收盘价"] == 10.2

    def test_excel_save_and_load(self):
        """测试Excel格式保存和读取"""
        # 保存测试数据
        df = pd.DataFrame(TEST_HISTORY_KLINE)
        DataSaver.save(df, TEST_EXCEL_PATH, format="excel")
        # 验证文件存在
        assert os.path.exists(TEST_EXCEL_PATH)
        # 验证内容正确
        load_df = DataSaver.load(TEST_EXCEL_PATH, format="excel")
        assert len(load_df) == 2
        assert load_df.iloc[0]["交易日期"] == "2024-04-01"
        assert load_df.iloc[0]["收盘价"] == 10.2

    def test_empty_data_handle(self):
        """测试空数据处理，不会报错，不会创建空文件"""
        # 传入空列表
        result = DataSaver.save([], TEST_JSON_PATH, format="json", mode="cover", field_name="test")
        assert result is False
        # 传入空DataFrame
        df = pd.DataFrame()
        result = DataSaver.save(df, TEST_CSV_PATH, format="csv")
        assert result is False
        # 验证没有创建文件
        assert not os.path.exists("./data/test_empty.json")
        assert not os.path.exists("./data/test_empty.csv")

    def test_null_value_handle(self):
        """测试None/空值处理，不会报错，正常保存"""
        # 先清空文件
        if os.path.exists(TEST_JSON_PATH):
            os.remove(TEST_JSON_PATH)
        data_with_null = [
            {"交易日期": "2024-04-04", "开盘价": 10.8, "最高价": 11.2, "最低价": 10.5, "收盘价": None, "成交量(手)": 13000, "成交额(万)": None},
        ]
        DataSaver.save(data_with_null, TEST_JSON_PATH, format="json", mode="append", field_name="history_kline", unique_key="交易日期")
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert len(data["history_kline"]) == 1
        assert data["history_kline"][0]["收盘价"] is None

    def test_all_data_type_support(self):
        """测试支持所有4种数据类型（历史K线/行情/财务/资金流向）"""
        # 测试行情数据保存
        DataSaver.save(TEST_QUOTE, TEST_JSON_PATH, format="json", mode="cover", field_name="quote")
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["quote"]["当前价格"] == 10.5

        # 测试财务数据保存
        DataSaver.save(TEST_FINANCE, TEST_JSON_PATH, format="json", mode="append", field_name="finance", unique_key="报告期")
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert len(data["finance"]) == 2

        # 测试资金流向保存
        DataSaver.save(TEST_MONEY_FLOW, TEST_JSON_PATH, format="json", mode="append", field_name="money_flow", unique_key="时间")
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert len(data["money_flow"]) == 2

    def test_unit_conversion_auto(self):
        """测试数值类字段自动单位转换：元转万元，百分比保留2位小数"""
        # 传入单位为元的原始数据
        raw_data = [
            {"交易日期": "2024-04-05", "开盘价": 11.0, "收盘价": 11.5, "成交额": 23000000, "涨跌幅": 0.065123, "换手率": 0.003456}
        ]
        DataSaver.save(raw_data, TEST_JSON_PATH, format="json", mode="append", field_name="history_kline", unique_key="交易日期")
        with open(TEST_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        last_item = data["history_kline"][-1]
        # 验证单位转换正确：23000000元 → 2300.0万元
        assert last_item["成交额(万)"] == 2300.0
        # 验证百分比保留2位小数：6.51%，0.35%
        assert last_item["涨跌幅(%)"] == 6.51
        assert last_item["换手率(%)"] == 0.35
