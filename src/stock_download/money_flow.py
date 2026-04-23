from datetime import datetime
from typing import List, Optional

import pandas as pd

from .client import XueqiuClient
from .config import UNIT_CONVERT
from .utils import DataSaver, validate_moneyflow_data, validate_stock_code, write_quality_report


class MoneyFlowFetcher:
    """资金流向数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()

    def get_stock_money_flow(self, symbol: str) -> pd.DataFrame:
        """获取单只股票的资金流向分钟级数据
        :param symbol: 股票代码
        :return: 资金流向DataFrame
        """
        # 校验股票代码
        valid, msg = validate_stock_code(symbol)
        if not valid:
            raise Exception(f"股票代码{symbol}校验失败：{msg}")

        raw_data = self.client.get_money_flow_minute(symbol)
        if not raw_data or "data" not in raw_data or not raw_data["data"]["items"]:
            raise Exception(f"获取股票{symbol}资金流向失败")

        items = raw_data["data"]["items"]
        result = []
        prev_amount = 0  # 上一分钟的累计值

        for item in items:
            timestamp = item["timestamp"]
            # 时间戳转成北京时间
            time_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M')
            current_amount = item["amount"]

            # 差分计算单分钟净流入：当前累计 - 上一分钟累计，保留原始值不转换
            minute_amount = current_amount - prev_amount
            prev_amount = current_amount

            result.append({
                "时间": time_str,
                "资金流向(元)": minute_amount
            })

        # 最后加一行汇总：最后一分钟的累计值就是当日总净流入，保留原始值
        total_amount = items[-1]["amount"] if items else 0
        result.append({
            "时间": "今日汇总",
            "资金流向(元)": total_amount
        })

        # 数据质量校验
        df = pd.DataFrame(result)
        valid, anomalies = validate_moneyflow_data(df.to_dict("records"))
        if not valid:
            write_quality_report(anomalies)
            print(f"股票{symbol}资金流向数据存在异常，已记录质量报告")
        return df

    def get_north_money(self) -> dict:
        """获取北向资金数据"""
        raw_data = self.client.get_north_money()
        if not raw_data or "data" not in raw_data:
            raise Exception("获取北向资金数据失败")

        data = raw_data["data"]
        return {
            "北向资金净流入(亿)": round(data.get("north_inflow", 0) / UNIT_CONVERT["yuan_to_yi"], UNIT_CONVERT["decimal_places"]),
            "南向资金净流入(亿)": round(data.get("south_inflow", 0) / UNIT_CONVERT["yuan_to_yi"], UNIT_CONVERT["decimal_places"]),
            "沪股通净流入(亿)": round(data.get("hkt_sh_inflow", 0) / UNIT_CONVERT["yuan_to_yi"], UNIT_CONVERT["decimal_places"]),
            "深股通净流入(亿)": round(data.get("hkt_sz_inflow", 0) / UNIT_CONVERT["yuan_to_yi"], UNIT_CONVERT["decimal_places"]),
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
            symbol = item.get("symbol")
            stock_name = item.get("name", "")

            # 过滤不支持的股票
            valid, _ = validate_stock_code(symbol)
            if not valid or "ST" in stock_name or "*ST" in stock_name:
                continue

            result.append({
                "股票代码": symbol,
                "股票名称": stock_name,
                "收盘价": item.get("close"),
                "涨跌幅(%)": item.get("percent"),
                "龙虎榜净买入(万)": round(item.get("net_buy_amount", 0) / UNIT_CONVERT["yuan_to_wan"], UNIT_CONVERT["decimal_places"]),
                "龙虎榜买入总额(万)": round(item.get("buy_amount", 0) / UNIT_CONVERT["yuan_to_wan"], UNIT_CONVERT["decimal_places"]),
                "龙虎榜卖出总额(万)": round(item.get("sell_amount", 0) / UNIT_CONVERT["yuan_to_wan"], UNIT_CONVERT["decimal_places"]),
                "上榜原因": item.get("reason"),
                "上榜日期": item.get("date")
            })

        return pd.DataFrame(result)

    def save_data(self, data: pd.DataFrame | dict, file_path: str, format: str = "json") -> bool:
        """保存资金流向数据到文件
        :param data: 要保存的数据，支持DataFrame或字典列表
        :param file_path: 保存文件路径
        :param format: 保存格式：json/csv/excel
        :return: 保存成功返回True
        """
        return DataSaver.save(data, file_path, format=format)

    def get_batch_stock_money_flow(self, symbols: List[str]) -> pd.DataFrame:
        """批量获取多只股票的资金流向数据
        :param symbols: 股票代码列表
        :return: 合并的资金流向DataFrame，包含股票代码列
        """
        result = []
        for symbol in symbols:
            try:
                df = self.get_stock_money_flow(symbol)
                df["股票代码"] = symbol
                result.append(df)
            except Exception as e:
                print(f"获取股票{symbol}资金流向失败：{str(e)}")
                continue
        return pd.concat(result, ignore_index=True) if result else pd.DataFrame()
