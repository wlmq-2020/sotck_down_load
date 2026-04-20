#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自选股quote-update功能测试用例
覆盖所有核心功能：数据完整性、自动补全、增量去重
"""
import os
import json
import random
import shutil
import subprocess
import pandas as pd

# 配置
QUOTE_DIR = "./data/quote/"
CUSTOM_STOCK_FILE = "./data/自选股票列表.csv"

# 必须存在的核心字段定义，只要缺一个就算更新失败
REQUIRED_ROOT_FIELDS = ["quote", "history_kline", "finance", "money_flow"]
QUOTE_REQUIRED_KEYS = ["股票代码", "当前价格", "涨跌幅(%)", "开盘价", "最高价", "最低价", "成交量(手)", "成交额(万)"]
KLINE_REQUIRED_KEYS = ["交易日期", "收盘价", "涨跌幅(%)", "成交量(手)", "成交额(万)"]
FINANCE_REQUIRED_KEYS = ["报告期", "营业收入(万元)", "净利润(万元)", "扣非净利润(万元)"]
MONEY_FLOW_REQUIRED_KEYS = ["时间", "资金流向(万元)"]

def get_custom_stock_list():
    """获取自选股票列表"""
    if not os.path.exists(CUSTOM_STOCK_FILE):
        raise Exception("自选股票列表文件不存在")
    df = pd.read_csv(CUSTOM_STOCK_FILE, dtype={"full_code": str})
    return df["full_code"].tolist()

def run_quote_update():
    """执行quote-update命令"""
    # Windows系统编码兼容，只判断返回码，不解析输出
    result = subprocess.run(
        ["python", "main.py", "quote-update"],
        capture_output=True,
        text=True
    )
    return result.returncode == 0

def check_stock_data_complete(code):
    """检查单只股票的数据完整性，只要缺核心字段就算失败"""
    json_path = os.path.join(QUOTE_DIR, f"{code}.json")
    if not os.path.exists(json_path):
        return False, f"JSON文件不存在：{json_path}"

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 检查根字段
    for field in REQUIRED_ROOT_FIELDS:
        if field not in data:
            return False, f"缺少根字段：{field}"
        if not data[field]:
            return False, f"根字段为空：{field}"

    # 检查quote字段
    quote = data["quote"]
    for key in QUOTE_REQUIRED_KEYS:
        if key not in quote:
            return False, f"quote缺少关键字段：{key}"

    # 检查history_kline字段，至少要有一条数据
    kline_list = data["history_kline"]
    if len(kline_list) == 0:
        return False, "history_kline为空"
    for key in KLINE_REQUIRED_KEYS:
        if key not in kline_list[0]:
            return False, f"history_kline缺少关键字段：{key}"

    # 检查finance字段，至少要有一条数据
    finance_list = data["finance"]
    if len(finance_list) == 0:
        return False, "finance为空"
    for key in FINANCE_REQUIRED_KEYS:
        if key not in finance_list[0]:
            return False, f"finance缺少关键字段：{key}"

    # 检查money_flow字段，至少要有一条数据
    money_list = data["money_flow"]
    if len(money_list) == 0:
        return False, "money_flow为空"
    for key in MONEY_FLOW_REQUIRED_KEYS:
        if key not in money_list[0]:
            return False, f"money_flow缺少关键字段：{key}"

    return True, "数据完整"

def test_all_stocks_data_complete():
    """测试用例1：执行quote-update后，所有自选股数据完整"""
    print("="*60)
    print("测试用例1：所有自选股数据完整性检查")
    print("="*60)

    # 先执行更新
    print("1. 执行quote-update命令...")
    success = run_quote_update()
    assert success, "quote-update命令执行失败"
    print("【成功】 quote-update执行成功")

    # 获取所有自选股
    stocks = get_custom_stock_list()
    print(f"2. 找到{len(stocks)}只自选股，开始检查数据完整性...")

    all_pass = True
    for code in stocks:
        ok, msg = check_stock_data_complete(code)
        if ok:
            print(f"【成功】 {code} 数据完整")
        else:
            print(f"【失败】 {code} {msg}")
            all_pass = False

    assert all_pass, "存在股票数据不完整，更新失败"
    print("【成功】 所有自选股数据完整，测试通过")

def test_missing_stock_auto_replenish():
    """测试用例2：随机删除一只自选股数据，执行更新后自动补全"""
    print("\n" + "="*60)
    print("测试用例2：缺失股票自动补全功能")
    print("="*60)

    stocks = get_custom_stock_list()
    # 随机选一个股票删除
    test_code = random.choice(stocks)
    json_path = os.path.join(QUOTE_DIR, f"{test_code}.json")
    backup_path = f"{json_path}.bak"

    try:
        # 备份原文件
        shutil.copy(json_path, backup_path)
        # 删除文件
        os.remove(json_path)
        print(f"1. 已随机删除股票：{test_code}，原文件已备份")
        assert not os.path.exists(json_path), "删除文件失败"

        # 执行更新
        print("2. 执行quote-update命令...")
        success = run_quote_update()
        assert success, "quote-update命令执行失败"
        print("【成功】 quote-update执行成功")

        # 检查文件是否重新生成且数据完整
        print("3. 检查数据是否自动补全...")
        ok, msg = check_stock_data_complete(test_code)
        assert ok, f"自动补全失败：{msg}"
        print(f"【成功】 {test_code} 数据已自动补全，所有核心字段完整")

    finally:
        # 恢复原文件，不影响用户数据
        if os.path.exists(backup_path):
            shutil.move(backup_path, json_path)
            print("4. 已恢复原有数据文件")

def test_increment_update_no_duplicate():
    """测试用例3：增量更新不会产生重复数据"""
    print("\n" + "="*60)
    print("测试用例3：增量更新去重功能")
    print("="*60)

    stocks = get_custom_stock_list()
    test_code = random.choice(stocks)
    json_path = os.path.join(QUOTE_DIR, f"{test_code}.json")

    # 先获取更新前的K线数量
    with open(json_path, "r", encoding="utf-8") as f:
        before_data = json.load(f)
    before_kline_count = len(before_data["history_kline"])
    before_dates = set(item["交易日期"] for item in before_data["history_kline"])
    print(f"1. 测试股票：{test_code}，更新前K线数量：{before_kline_count}，去重后日期数：{len(before_dates)}")

    # 重复执行更新
    print("2. 重复执行quote-update命令...")
    success = run_quote_update()
    assert success, "quote-update命令执行失败"
    print("【成功】 quote-update执行成功")

    # 检查更新后的数据
    with open(json_path, "r", encoding="utf-8") as f:
        after_data = json.load(f)
    after_kline_count = len(after_data["history_kline"])
    after_dates = set(item["交易日期"] for item in after_data["history_kline"])
    print(f"3. 更新后K线数量：{after_kline_count}，去重后日期数：{len(after_dates)}")

    # 验证没有重复数据：去重后日期数应该等于K线数量
    assert len(after_dates) == after_kline_count, "存在重复的交易日期，去重逻辑失效"
    # 验证更新后没有减少数据
    assert after_kline_count >= before_kline_count, "更新后数据反而减少，逻辑错误"
    print("【成功】 增量更新正常，没有重复数据，测试通过")

if __name__ == "__main__":
    try:
        test_all_stocks_data_complete()
        test_missing_stock_auto_replenish()
        test_increment_update_no_duplicate()
        print("\n" + "="*60)
        print("【全部通过】 所有测试用例全部通过！quote-update功能完全正常")
        print("="*60)
    except AssertionError as e:
        print(f"\n【失败】 测试失败：{str(e)}")
        exit(1)
    except Exception as e:
        print(f"\n【失败】 测试执行出错：{str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)
