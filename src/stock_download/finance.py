
import pandas as pd

from .client import XueqiuClient


class FinanceFetcher:
    """财务基本面数据获取器"""

    def __init__(self):
        self.client = XueqiuClient()

    def get_finance_report(self, symbol: str, report_type: str = "all") -> pd.DataFrame:
        """获取财务报表数据
        :param symbol: 股票代码
        :param report_type: 报表类型：all(全部), income(利润表), balance(资产负债表), cash(现金流量表)
        :return: DataFrame格式的财报数据
        """
        if report_type == "income" or report_type == "all":
            raw_data = self.client.get_income(symbol)
            if not raw_data or "data" not in raw_data:
                raise Exception(f"获取股票{symbol}利润表失败")

            reports = []
            for item in raw_data["data"]["list"]:
                report = {
                    "报告期": item.get("report_name"),
                    "营业收入(元)": item.get("total_revenue")[0],
                    "营业收入同比(%)": round(item.get("total_revenue")[1] * 100, 2) if item.get("total_revenue")[1] else None,
                    "净利润(元)": item.get("net_profit")[0],
                    "净利润同比(%)": round(item.get("net_profit")[1] * 100, 2) if item.get("net_profit")[1] else None,
                    "扣非净利润(元)": item.get("net_profit_atsopc")[0],
                    "扣非净利润同比(%)": round(item.get("net_profit_atsopc")[1] * 100, 2) if item.get("net_profit_atsopc")[1] else None,
                    "营业利润(元)": item.get("op")[0],
                    "营业利润同比(%)": round(item.get("op")[1] * 100, 2) if item.get("op")[1] else None
                }
                reports.append(report)

            df = pd.DataFrame(reports)
            return df


    def save_finance_data(self, data: pd.DataFrame, output_path: str):
        """保存财务数据到文件"""
        if output_path.endswith(".csv"):
            data.to_csv(output_path, index=False, encoding="utf_8_sig")
        elif output_path.endswith(".xlsx"):
            data.to_excel(output_path, index=False)
        elif output_path.endswith(".json"):
            data.to_json(output_path, orient="records", force_ascii=False)
        else:
            raise ValueError("不支持的文件格式，仅支持.csv/.xlsx/.json")
