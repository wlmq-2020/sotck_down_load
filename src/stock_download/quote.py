from typing import List

import pandas as pd

from .client import XueqiuClient
from .config import DATA_SCOPE, UNIT_CONVERT


class QuoteFetcher:
    """实时行情数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()

    def get_single_quote(self, symbol: str) -> dict:
        """获取单只股票的实时行情
        :param symbol: 股票代码，前缀式，比如SZ000001，SH600000
        :return: 格式化的行情数据
        """
        raw_data = self.client.get_quote(symbol)
        if not raw_data or "data" not in raw_data or not raw_data["data"][0]:
            raise Exception(f"获取股票{symbol}行情失败，请检查代码格式是否正确（前缀式：SZ000001/SH600000）")

        data = raw_data["data"][0]

        # 过滤ST股票
        stock_name = data.get("name", "")
        if "ST" in stock_name or "*ST" in stock_name:
            raise Exception(f"股票{symbol}({stock_name})是ST股票，默认不支持获取，请在配置中单独开启")

        # 直接返回接口原始字段，不做任何单位转换或数值修改
        return {
            "股票代码": data.get("symbol"),
            "当前价格": data.get("current"),
            "涨跌幅(%)": data.get("percent", 0),
            "涨跌额": data.get("chg"),
            "开盘价": data.get("open"),
            "最高价": data.get("high"),
            "最低价": data.get("low"),
            "昨收价": data.get("last_close"),
            "成交量(手)": data.get("volume"),
            "成交额(元)": data.get("amount", 0),
            "换手率(%)": data.get("turnover_rate", 0),
            "振幅(%)": data.get("amplitude", 0),
            "年初至今涨跌幅(%)": data.get("current_year_percent", 0),
            "总市值(元)": data.get("market_capital", 0),
            "流通市值(元)": data.get("float_market_capital", 0),
            "平均价格": data.get("avg_price"),
            "更新时间": data.get("timestamp")
        }

    def get_batch_quotes(self, symbols: List[str]) -> pd.DataFrame:
        """批量获取多只股票的实时行情
        :param symbols: 股票代码列表
        :return: DataFrame格式的行情数据
        """
        result = []
        for symbol in symbols:
            try:
                quote = self.get_single_quote(symbol)
                result.append(quote)
            except Exception as e:
                print(f"获取股票{symbol}行情失败：{str(e)}")
                continue

        return pd.DataFrame(result)


    def get_history_kline(self, symbol: str, days: int = DATA_SCOPE["default_kline_days"]) -> pd.DataFrame:
        """获取股票历史日K数据
        :param symbol: 股票代码，前缀式，比如SZ000001，SH600000
        :param days: 获取天数，默认1825天≈5年
        :return: DataFrame格式的历史K线数据
        """
        raw_data = self.client.get_kline(symbol, 'day', days)
        if not raw_data or "data" not in raw_data or "item" not in raw_data["data"]:
            raise Exception(f"获取股票{symbol}历史K线失败")

        # 字段映射：雪球K线返回的24个字段完整对应，全部保留不丢弃
        columns = [
            "时间戳", "成交量(手)", "开盘价", "最高价", "最低价", "收盘价",
            "涨跌额", "涨跌幅(%)", "换手率(%)", "成交额(元)", "量比", "市盈率(TTM)",
            "市净率", "总市值(元)", "流通市值(元)", "涨速(%)", "5分钟涨跌幅(%)",
            "内盘(手)", "外盘(手)", "委买量(手)", "委卖量(手)", "委差(手)",
            "均价(元)", "昨收价"
        ]

        df = pd.DataFrame(raw_data["data"]["item"], columns=columns)

        # 仅新增交易日期字段，其他所有数据完全使用接口原始返回值，不做任何修改、转换、round
        df["交易日期"] = pd.to_datetime(df["时间戳"], unit="ms").dt.strftime("%Y-%m-%d")

        # 保留全部原始字段+交易日期，不做任何数值修改
        result_df = df[[
            "交易日期", "开盘价", "最高价", "最低价", "收盘价",
            "成交量(手)", "成交额(元)", "涨跌幅(%)", "涨跌额", "换手率(%)",
            "量比", "市盈率(TTM)", "市净率", "总市值(元)", "流通市值(元)",
            "涨速(%)", "5分钟涨跌幅(%)", "内盘(手)", "外盘(手)",
            "委买量(手)", "委卖量(手)", "委差(手)", "均价(元)", "昨收价", "时间戳"
        ]].sort_values("交易日期", ascending=True).reset_index(drop=True)

        return result_df
