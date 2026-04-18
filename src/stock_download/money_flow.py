from typing import Optional

import pandas as pd

from .client import XueqiuClient


class MoneyFlowFetcher:
    """资金流向数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()

    def get_stock_money_flow(self, symbol: str) -> pd.DataFrame:
        """获取单只股票的资金流向分钟级数据
        :param symbol: 股票代码
        :return: 资金流向DataFrame
        """
        raw_data = self.client.get_money_flow_minute(symbol)
        if not raw_data or "data" not in raw_data or not raw_data["data"]["items"]:
            raise Exception(f"获取股票{symbol}资金流向失败")

        items = raw_data["data"]["items"]
        result = []
        total_inflow = 0
        total_outflow = 0

        for item in items:
            timestamp = item["timestamp"]
            # 时间戳转成北京时间
            from datetime import datetime
            time_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M')
            amount = item["amount"]

            # 统计流入流出
            if amount > 0:
                total_inflow += amount
            else:
                total_outflow += abs(amount)

            result.append({
                "时间": time_str,
                "资金流向(万元)": round(amount / 10000, 2)
            })

        # 最后加一行汇总
        result.append({
            "时间": "今日汇总",
            "资金流向(万元)": round((total_inflow - total_outflow) / 10000, 2)
        })

        return pd.DataFrame(result)

    def get_north_money(self) -> dict:
        """获取北向资金数据"""
        raw_data = self.client.get_north_money()
        if not raw_data or "data" not in raw_data:
            raise Exception("获取北向资金数据失败")

        data = raw_data["data"]
        return {
            "北向资金净流入(亿)": round(data.get("north_inflow", 0) / 100000000, 2),
            "南向资金净流入(亿)": round(data.get("south_inflow", 0) / 100000000, 2),
            "沪股通净流入(亿)": round(data.get("hkt_sh_inflow", 0) / 100000000, 2),
            "深股通净流入(亿)": round(data.get("hkt_sz_inflow", 0) / 100000000, 2),
            "更新时间": data.get("update_time")
        }

    def get_lhb_data(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取龙虎榜数据
        :param date: 日期，格式YYYY-MM-DD，不传默认最新
        :return: 龙虎榜数据DataFrame
        """
        raw_data = self.client.get_lhb_data(date)
        if not raw_data or "data" not in raw_data:
            raise Exception("获取龙虎榜数据失败")

        result = []
        for item in raw_data["data"]["items"]:
            result.append({
                "股票代码": item.get("symbol"),
                "股票名称": item.get("name"),
                "收盘价": item.get("close"),
                "涨跌幅(%)": item.get("percent"),
                "龙虎榜净买入(万)": round(item.get("net_buy_amount", 0) / 10000, 2),
                "龙虎榜买入总额(万)": round(item.get("buy_amount", 0) / 10000, 2),
                "龙虎榜卖出总额(万)": round(item.get("sell_amount", 0) / 10000, 2),
                "上榜原因": item.get("reason"),
                "上榜日期": item.get("date")
            })

        return pd.DataFrame(result)

    def save_money_flow_data(self, data, output_path: str):
        """保存资金流向数据到文件"""
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
