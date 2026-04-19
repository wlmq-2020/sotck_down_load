
import pandas as pd

from .client import XueqiuClient
from .config import INDEX_CONFIG, UNIT_CONVERT
from .quote import QuoteFetcher


class IndexFundFetcher:
    """指数与基金数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()
        self.quote_fetcher = QuoteFetcher()

        # 常用大盘指数代码
        self.MAJOR_INDEX = INDEX_CONFIG

    def get_major_index_quotes(self) -> pd.DataFrame:
        """获取主要大盘指数行情"""
        symbols = list(self.MAJOR_INDEX.keys())
        df = self.quote_fetcher.get_batch_quotes(symbols)
        # 替换指数名称
        df["股票名称"] = df["股票代码"].map(self.MAJOR_INDEX)
        return df

    def get_industry_plate(self) -> pd.DataFrame:
        """获取行业板块行情数据"""
        raw_data = self.client.get_industry_plate()
        if not raw_data or "data" not in raw_data:
            raise Exception("获取行业板块数据失败")

        result = []
        for item in raw_data["data"]["list"]:
            result.append({
                "板块代码": item.get("symbol"),
                "板块名称": item.get("name"),
                "当前价格": item.get("current"),
                "涨跌幅(%)": round(item.get("percent", 0) * UNIT_CONVERT["percent_multiply"], UNIT_CONVERT["decimal_places"]),
                "上涨家数": item.get("rise_count"),
                "下跌家数": item.get("fall_count"),
                "领涨股": item.get("lead_stock_name"),
                "领涨股涨跌幅(%)": round(item.get("lead_stock_percent", 0) * UNIT_CONVERT["percent_multiply"], UNIT_CONVERT["decimal_places"])
            })

        return pd.DataFrame(result)

    def get_fund_net_value(self, fund_code: str) -> dict:
        """获取基金净值数据
        :param fund_code: 基金代码，比如510300.SH（沪深300ETF）
        :return: 基金净值数据
        """
        raw_data = self.client.get_fund_net_value(fund_code)
        if not raw_data or "data" not in raw_data:
            raise Exception(f"获取基金{fund_code}净值失败")

        data = raw_data["data"]
        return {
            "基金代码": fund_code,
            "基金名称": data.get("name"),
            "单位净值": data.get("nav"),
            "累计净值": data.get("accumulated_nav"),
            "涨跌幅(%)": round(data.get("nav_percent", 0) * UNIT_CONVERT["percent_multiply"], UNIT_CONVERT["decimal_places"]),
            "净值日期": data.get("nav_date"),
            "基金类型": data.get("type"),
            "基金规模(亿)": round(data.get("fund_scale", 0) / UNIT_CONVERT["yuan_to_yi"], UNIT_CONVERT["decimal_places"]),
            "成立日期": data.get("establish_date")
        }

    def get_etf_quote(self, etf_code: str) -> dict:
        """获取ETF实时行情
        :param etf_code: ETF代码，比如510300.SH
        :return: ETF行情数据（和股票行情格式一致）
        """
        return self.quote_fetcher.get_single_quote(etf_code)
