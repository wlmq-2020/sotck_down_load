from typing import List, Union

import pandas as pd

from .client import XueqiuClient


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

        # 格式化返回关键字段
        return {
            "股票代码": data.get("symbol"),
            "当前价格": data.get("current"),
            "涨跌幅(%)": round(data.get("percent", 0), 2),
            "涨跌额": data.get("chg"),
            "开盘价": data.get("open"),
            "最高价": data.get("high"),
            "最低价": data.get("low"),
            "昨收价": data.get("last_close"),
            "成交量(手)": data.get("volume"),
            "成交额(万)": round(data.get("amount", 0) / 10000, 2),
            "换手率(%)": round(data.get("turnover_rate", 0), 2),
            "振幅(%)": round(data.get("amplitude", 0), 2),
            "年初至今涨跌幅(%)": round(data.get("current_year_percent", 0), 2),
            "总市值(亿)": round(data.get("market_capital", 0) / 100000000, 2),
            "流通市值(亿)": round(data.get("float_market_capital", 0) / 100000000, 2),
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

    def save_quote(self, data: Union[dict, pd.DataFrame], output_path: str):
        """保存行情数据到文件
        :param data: 行情数据，可以是dict（单只）或DataFrame（批量）
        :param output_path: 输出文件路径，支持.csv/.xlsx/.json
        """
        if isinstance(data, dict):
            data = pd.DataFrame([data])

        if output_path.endswith(".csv"):
            data.to_csv(output_path, index=False, encoding="utf_8_sig")
        elif output_path.endswith(".xlsx"):
            data.to_excel(output_path, index=False)
        elif output_path.endswith(".json"):
            data.to_json(output_path, orient="records", force_ascii=False)
        else:
            raise ValueError("不支持的文件格式，仅支持.csv/.xlsx/.json")
