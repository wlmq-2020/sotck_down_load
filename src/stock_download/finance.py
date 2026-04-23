
from typing import List

import pandas as pd

from .client import XueqiuClient
from .utils import DataSaver, validate_finance_data, validate_stock_code, write_quality_report


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
        # 校验股票代码
        valid, msg = validate_stock_code(symbol)
        if not valid:
            raise Exception(f"股票代码{symbol}校验失败：{msg}")

        all_reports = {}

        # 获取利润表
        # 数据来源：雪球接口原始值，无任何修改
        if report_type in ["income", "all"]:
            raw_data = self.client.get_income(symbol)
            if not raw_data or "data" not in raw_data:
                raise Exception(f"获取股票{symbol}利润表失败")

            for item in raw_data["data"]["list"]:
                report_period = item.get("report_name")
                if report_period not in all_reports:
                    all_reports[report_period] = {"报告期": report_period}

                total_revenue = item.get("total_revenue")
                net_profit = item.get("net_profit")
                net_profit_atsopc = item.get("net_profit_atsopc")
                op = item.get("op")

                all_reports[report_period].update({
                    "营业收入": total_revenue[0] if total_revenue and len(total_revenue) > 0 else None,
                    "营业收入同比": total_revenue[1] if total_revenue and len(total_revenue) > 1 else None,
                    "净利润": net_profit[0] if net_profit and len(net_profit) > 0 else None,
                    "净利润同比": net_profit[1] if net_profit and len(net_profit) > 1 else None,
                    "扣非净利润": net_profit_atsopc[0] if net_profit_atsopc and len(net_profit_atsopc) > 0 else None,
                    "扣非净利润同比": net_profit_atsopc[1] if net_profit_atsopc and len(net_profit_atsopc) > 1 else None,
                    "营业利润": op[0] if op and len(op) > 0 else None,
                    "营业利润同比": op[1] if op and len(op) > 1 else None
                })

        # 获取资产负债表
        # 数据来源：雪球接口原始值，无任何修改
        if report_type in ["balance", "all"]:
            raw_data = self.client.get_balance(symbol)
            if not raw_data or "data" not in raw_data:
                raise Exception(f"获取股票{symbol}资产负债表失败")

            for item in raw_data["data"]["list"]:
                report_period = item.get("report_name")
                if report_period not in all_reports:
                    all_reports[report_period] = {"报告期": report_period}

                total_assets = item.get("total_assets")
                total_liability = item.get("total_liability")
                total_equity = item.get("total_equity")
                cash = item.get("cash")
                account_receivable = item.get("account_receivable")
                inventory = item.get("inventory")

                all_reports[report_period].update({
                    "总资产": total_assets[0] if total_assets and len(total_assets) > 0 else None,
                    "总资产同比": total_assets[1] if total_assets and len(total_assets) > 1 else None,
                    "总负债": total_liability[0] if total_liability and len(total_liability) > 0 else None,
                    "总负债同比": total_liability[1] if total_liability and len(total_liability) > 1 else None,
                    "净资产": total_equity[0] if total_equity and len(total_equity) > 0 else None,
                    "净资产同比": total_equity[1] if total_equity and len(total_equity) > 1 else None,
                    "货币资金": cash[0] if cash and len(cash) > 0 else None,
                    "应收账款": account_receivable[0] if account_receivable and len(account_receivable) > 0 else None,
                    "存货": inventory[0] if inventory and len(inventory) > 0 else None
                })

        # 获取现金流量表
        # 数据来源：雪球接口原始值，无任何修改
        if report_type in ["cash", "all"]:
            raw_data = self.client.get_cash_flow(symbol)
            if not raw_data or "data" not in raw_data:
                raise Exception(f"获取股票{symbol}现金流量表失败")

            for item in raw_data["data"]["list"]:
                report_period = item.get("report_name")
                if report_period not in all_reports:
                    all_reports[report_period] = {"报告期": report_period}

                net_operate_cash_flow = item.get("net_operate_cash_flow")
                net_invest_cash_flow = item.get("net_invest_cash_flow")
                net_finance_cash_flow = item.get("net_finance_cash_flow")
                cash_increase = item.get("cash_increase")

                all_reports[report_period].update({
                    "经营活动现金流": net_operate_cash_flow[0] if net_operate_cash_flow and len(net_operate_cash_flow) > 0 else None,
                    "经营活动现金流同比": net_operate_cash_flow[1] if net_operate_cash_flow and len(net_operate_cash_flow) > 1 else None,
                    "投资活动现金流": net_invest_cash_flow[0] if net_invest_cash_flow and len(net_invest_cash_flow) > 0 else None,
                    "筹资活动现金流": net_finance_cash_flow[0] if net_finance_cash_flow and len(net_finance_cash_flow) > 0 else None,
                    "现金净增加额": cash_increase[0] if cash_increase and len(cash_increase) > 0 else None
                })

        # 转换为DataFrame
        reports_list = list(all_reports.values())
        df = pd.DataFrame(reports_list).sort_values("报告期", ascending=False).reset_index(drop=True)

        # 数据质量校验
        valid, anomalies = validate_finance_data(df.to_dict("records"))
        if not valid:
            write_quality_report(anomalies)
            print(f"股票{symbol}财务数据存在异常，已记录质量报告")

        return df

    def save_data(self, data: pd.DataFrame | dict, file_path: str, format: str = "json") -> bool:
        """保存财务数据到文件
        :param data: 要保存的数据，支持DataFrame或字典列表
        :param file_path: 保存文件路径
        :param format: 保存格式：json/csv/excel
        :return: 保存成功返回True
        """
        return DataSaver.save(data, file_path, format=format)

    def get_batch_finance_report(self, symbols: List[str], report_type: str = "all") -> pd.DataFrame:
        """批量获取多只股票的财务数据
        :param symbols: 股票代码列表
        :param report_type: 报表类型
        :return: 合并的财务数据DataFrame，包含股票代码列
        """
        result = []
        for symbol in symbols:
            try:
                df = self.get_finance_report(symbol, report_type)
                df["股票代码"] = symbol
                result.append(df)
            except Exception as e:
                print(f"获取股票{symbol}财务数据失败：{str(e)}")
                continue
        return pd.concat(result, ignore_index=True) if result else pd.DataFrame()



