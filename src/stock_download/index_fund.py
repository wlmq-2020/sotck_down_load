
import pandas as pd

from .client import XueqiuClient
from .quote import QuoteFetcher


class IndexFundFetcher:
    """指数与基金数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()
        self.quote_fetcher = QuoteFetcher()

        # 常用大盘指数代码
        self.MAJOR_INDEX = {
            "SH000001": "上证指数",
            "SZ399001": "深证成指",
            "SZ399006": "创业板指",
            "SH000300": "沪深300",
            "SH000016": "上证50",
            "SZ399005": "中小板指",
            "SH000905": "中证500",
            "HKHSI": "恒生指数",
            "DJI": "道琼斯指数",
            "IXIC": "纳斯达克",
            "SPX": "标普500"
        }

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
                "涨跌幅(%)": round(item.get("percent", 0) * 100, 2),
                "上涨家数": item.get("rise_count"),
                "下跌家数": item.get("fall_count"),
                "领涨股": item.get("lead_stock_name"),
                "领涨股涨跌幅(%)": round(item.get("lead_stock_percent", 0) * 100, 2)
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
            "涨跌幅(%)": round(data.get("nav_percent", 0) * 100, 2),
            "净值日期": data.get("nav_date"),
            "基金类型": data.get("type"),
            "基金规模(亿)": round(data.get("fund_scale", 0) / 100000000, 2),
            "成立日期": data.get("establish_date")
        }

    def get_etf_quote(self, etf_code: str) -> dict:
        """获取ETF实时行情
        :param etf_code: ETF代码，比如510300.SH
        :return: ETF行情数据（和股票行情格式一致）
        """
        return self.quote_fetcher.get_single_quote(etf_code)

    def save_index_fund_data(self, data, output_path: str):
        """保存指数基金数据到文件"""
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
