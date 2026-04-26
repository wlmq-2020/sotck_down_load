#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""统一配置模块，所有可配置参数集中在此管理"""
import os
from typing import Dict, List, Union

from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# -------------------------------
# 1. 防封策略配置
# -------------------------------
ANTI_CRAWL: Dict[str, Union[float, int, List[int]]] = {
    # 请求延迟范围（秒）
    "delay_min": float(os.getenv("REQUEST_DELAY_MIN", 1.0)),
    "delay_max": float(os.getenv("REQUEST_DELAY_MAX", 2.0)),
    # 每分钟最大请求数
    "max_requests_per_minute": int(os.getenv("MAX_REQUESTS_PER_MINUTE", 30)),
    # 最大重试次数
    "max_retry_times": int(os.getenv("MAX_RETRY_TIMES", 3)),
    # 封禁错误码（遇到直接停止）
    "ban_error_codes": [403, 401, 10001],
    # 重试错误码（遇到自动重试）
    "retry_error_codes": [500, 502, 503, 504, 400, 404],
}

# -------------------------------
# 2. 数据范围配置
# -------------------------------
DATA_SCOPE: Dict[str, Union[bool, int, List[str]]] = {
    # 是否过滤ST股票
    "filter_st": True,
    # 支持的股票前缀
    "support_prefix": ["SH", "SZ"],
    # 排除的股票代码前缀（北交所、科创板）
    "exclude_prefix": ["8", "4", "68"],
    # 默认历史K线获取天数（1825天≈5年）
    "default_kline_days": 1825,
    # 每日增量更新K线天数
    "daily_increment_kline_days": 30,
    # 周级全量补全K线天数
    "weekly_full_kline_days": 1825,
}

# -------------------------------
# 3. 单位转换配置
# -------------------------------
UNIT_CONVERT: Dict[str, Union[int, float]] = {
    # 元 → 万元
    "yuan_to_wan": 10000,
    # 元 → 亿元
    "yuan_to_yi": 100000000,
    # 小数 → 百分比乘数
    "percent_multiply": 100,
    # 数值保留小数位数
    "decimal_places": 2,
}

# -------------------------------
# 4. 路径配置
# -------------------------------
PATH: Dict[str, str] = {
    # 数据存储根目录
    "data_root": "./data/",
    # 各类型数据子目录
    "quote_dir": "./data/quote/",
    "finance_dir": "./data/finance/",
    "money_flow_dir": "./data/money_flow/",
    "index_fund_dir": "./data/index_fund/",
    "deep_data_dir": "./data/deep_data/",
    # 历史K线CSV存储目录
    "history_dir": "./data/history/",
    # 数据质量报告路径
    "quality_report_path": "./data/index/data_quality_report.csv",
    # 股票列表文件路径
    "stock_list_path": "./data/股票列表.csv",
}

# -------------------------------
# 5. 校验阈值配置
# -------------------------------
VALIDATION_THRESHOLD: Dict[str, Union[float, int]] = {
    # 普通股票最大涨跌幅（10%）
    "normal_stock_limit": 0.1,
    # ST股票最大涨跌幅（5%）
    "st_stock_limit": 0.05,
    # 涨跌幅校验误差允许范围
    "pct_tolerance": 0.1,
    # 百分比字段最大阈值（1000%）
    "max_percent_value": 1000,
    # 数值字段最大阈值（万亿级）
    "max_numeric_value": 1e12,
}

# -------------------------------
# 6. 定时任务配置
# -------------------------------
SCHEDULE: Dict[str, str] = {
    # 每日任务执行时间
    "daily_time": "17:00",
    # 每周任务执行时间（周日）
    "weekly_time": "02:00",
    # 每月任务执行时间（1号）
    "monthly_time": "03:00",
}

# -------------------------------
# 7. 常用指数配置
# -------------------------------
INDEX_CONFIG: Dict[str, str] = {
    "SH000001": "上证指数",
    "SZ399001": "深证成指",
    "SZ399006": "创业板指",
    "SH000300": "沪深300",
    "SH000016": "上证50",
    "SZ399005": "中小板指",
    "SH000905": "中证500",
    "DJI": "道琼斯工业指数",
    "IXIC": "纳斯达克综合指数",
    "SPX": "标普500",
}

# -------------------------------
# 8. 默认示例股票列表
# -------------------------------
DEFAULT_STOCKS: List[str] = [
    "SZ000001", "SH600000", "SH601318", "SZ002594", "SH600519"
]
