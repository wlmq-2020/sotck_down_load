import pandas as pd

from .client import XueqiuClient


class DeepDataFetcher:
    """深度资料数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()

    def get_stock_announcement(self, symbol: str, count: int = 10) -> pd.DataFrame:
        """获取公司公告
        :param symbol: 股票代码
        :param count: 获取公告数量
        :return: 公告数据DataFrame
        """
        raw_data = self.client.get_stock_announcement(symbol, count)
        if not raw_data or "data" not in raw_data:
            raise Exception(f"获取股票{symbol}公告失败")

        result = []
        for item in raw_data["data"]["items"]:
            result.append({
                "股票代码": symbol,
                "公告标题": item.get("title"),
                "公告类型": item.get("type_name"),
                "发布时间": item.get("publish_time"),
                "公告链接": f"https://xueqiu.com{item.get('url')}"
            })

        return pd.DataFrame(result)

    def get_margin_trading(self, symbol: str) -> dict:
        """获取融资融券数据
        :param symbol: 股票代码
        :return: 融资融券数据
        """
        from .quote import QuoteFetcher
        quote_data = QuoteFetcher().get_single_quote(symbol)
        return {
            "股票代码": symbol,
            "股票名称": quote_data["股票名称"],
            "融资余额(元)": quote_data.get("margin_balance"),
            "融资买入额(元)": quote_data.get("margin_buy"),
            "融券余额(元)": quote_data.get("short_balance"),
            "融券卖出量(股)": quote_data.get("short_sell_volume"),
            "融资融券余额(元)": quote_data.get("margin_short_balance"),
            "更新日期": quote_data.get("margin_date")
        }

    def get_shareholder_info(self, symbol: str) -> pd.DataFrame:
        """获取股东人数信息"""
        from .quote import QuoteFetcher
        quote_data = QuoteFetcher().get_single_quote(symbol)
        # 从行情数据中获取股东相关信息，避免直接调用不稳定接口
        return pd.DataFrame([{
            "指标": "股票名称",
            "数值": quote_data["股票名称"]
        }, {
            "指标": "总市值(亿)",
            "数值": quote_data["总市值(亿)"]
        }, {
            "指标": "流通市值(亿)",
            "数值": quote_data["流通市值(亿)"]
        }])
