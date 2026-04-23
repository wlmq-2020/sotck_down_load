#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""工具函数模块"""
import json
import os
import sys
from typing import Dict, List, Optional, Union

import pandas as pd

from .config import PATH, UNIT_CONVERT, VALIDATION_THRESHOLD


def check_root_dir_py_files() -> None:
    """
    检查项目根目录是否存在除main.py之外的其他py文件（符合规范要求：main.py同级不允许出现别的py文件）
    检测不通过则抛出异常，退出程序
    """
    # 获取项目根目录（main.py所在目录）
    root_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    # 遍历根目录下的文件
    illegal_files = []
    for filename in os.listdir(root_dir):
        file_path = os.path.join(root_dir, filename)
        # 只检查文件，不检查目录
        if os.path.isfile(file_path) and filename.endswith(".py"):
            if filename != "main.py":
                illegal_files.append(filename)

    if illegal_files:
        err_msg = f"""
错误：目录检测不通过，违反规范：main.py同级不允许出现别的py文件
违规文件列表：{', '.join(illegal_files)}
解决方案：
1. 将临时脚本/测试脚本移动到 ./temp/ 目录下
2. 不需要的文件直接删除
3. 功能模块必须放在 ./src/stock_download/ 目录下
"""
        print(err_msg)
        sys.exit(1)

    print("目录规范检测通过")

def init_project() -> tuple[list[str], str]:
    """初始化项目，创建所需目录、生成.env模板
    :return: 创建的目录列表，.env模板路径
    """
    # 创建需要的目录
    dirs = [
        "./data",
        "./logs",
        "./temp",
        "./tests",
        "./data/index"
    ]
    created_dirs = []
    for d in dirs:
        if not os.path.exists(d):
            os.makedirs(d)
            created_dirs.append(d)

    # 生成.env模板
    env_template_path = "./.env.template"
    env_created = False
    if not os.path.exists(env_template_path):
        env_content = """# 雪球Cookie，从浏览器登录雪球后获取
XUEQIU_COOKIE=your_xueqiu_cookie_here

# 请求延迟配置，单位秒
REQUEST_DELAY_MIN=1
REQUEST_DELAY_MAX=2

# 每分钟最多请求次数
MAX_REQUESTS_PER_MINUTE=30
"""
        with open(env_template_path, "w", encoding="utf-8") as f:
            f.write(env_content)
        env_created = True

    return created_dirs, env_template_path if env_created else None


def validate_stock_data(symbols: list[str], validate_type: str = "all",
                       start_date: str = "2021-01-01", end_date: str = None, debug: bool = False) -> tuple[list[dict], int, int]:
    """校验已下载的股票数据质量，自动修复缺失的文件和数据
    :param symbols: 股票代码列表
    :param validate_type: 校验数据类型：all/kline/quote/finance/money_flow
    :param start_date: K线校验开始日期
    :param end_date: K线校验结束日期，默认今日
    :param debug: 是否开启调试模式
    :return: 报告数据列表，成功计数，失败计数
    """
    from datetime import datetime

    from .quote import QuoteFetcher

    # 设置默认结束日期为今日
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    report_data = []
    success_count = 0
    fail_count = 0

    fetcher = QuoteFetcher()

    for symbol in symbols:
        try:
            # 读取已下载的K线数据
            json_path = f"./data/{symbol}.json"
            if not os.path.exists(json_path):
                # 自动修复：拉取全量数据创建文件
                try:
                    print(f"[INFO] 文件{json_path}不存在，自动拉取全量数据创建...")
                    success, _, _ = DataSaver.export_stock_json([symbol])
                    if success == 1:
                        # 创建成功，继续后续校验
                        report_item = {
                            "股票代码": symbol,
                            "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "异常类型": "已修复",
                            "异常日期": "",
                            "异常内容": f"文件不存在，已自动创建并拉取全量数据",
                            "状态": "已修复"
                        }
                        report_data.append(report_item)
                        # 重新读取文件
                        data = DataSaver.load(json_path, format='json')
                    else:
                        # 创建失败
                        report_item = {
                            '股票代码': symbol,
                            '校验时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            '异常类型': '文件不存在',
                            '异常日期': '',
                            '异常内容': f'JSON数据文件不存在：{json_path}，自动创建失败',
                            '状态': '失败'
                        }
                        report_data.append(report_item)
                        fail_count += 1
                        continue
                except Exception as e:
                    report_item = {
                        '股票代码': symbol,
                        '校验时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        '异常类型': '文件不存在',
                        '异常日期': '',
                        '异常内容': f'JSON数据文件不存在：{json_path}，自动创建失败：{str(e)}',
                        '状态': '失败'
                    }
                    report_data.append(report_item)
                    fail_count += 1
                    continue

            # 加载数据
            data = DataSaver.load(json_path, format='json')
            if not data:
                report_item = {
                    "股票代码": symbol,
                    "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "异常类型": "数据为空",
                    "异常日期": "",
                    "异常内容": "JSON文件内容为空",
                    "状态": "失败"
                }
                report_data.append(report_item)
                fail_count += 1
                continue

            # 初始化所有校验结果
            all_valid = True

            # ------------------- 校验K线数据 -------------------
            if validate_type in ['all', 'kline']:
                if 'kline' in data and data['kline']:
                    valid, anomalies, missing_dates = validate_kline_data(data['kline'], start_date, end_date)
                    if not valid:
                        all_valid = False
                        for anomaly in anomalies:
                            report_item = {
                                "股票代码": symbol,
                                "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "异常类型": anomaly["异常类型"],
                                "异常日期": anomaly["异常日期"],
                                "异常内容": anomaly["异常内容"],
                                "状态": "异常"
                            }
                            report_data.append(report_item)
                        fail_count += len(anomalies)

                        # K线自动修复
                        if missing_dates:
                            try:
                                # 计算需要下载的天数：从最早缺失日期到今日
                                min_missing_date = min(missing_dates)
                                days_diff = (datetime.now() - datetime.strptime(min_missing_date, "%Y-%m-%d")).days + 1

                                # 下载这段时间的全部K线
                                df_new = fetcher.get_history_kline(symbol, days=days_diff)
                                new_kline = df_new.to_dict(orient="records")

                                # 只保留缺失的日期
                                added_count = 0
                                for item in new_kline:
                                    if item['交易日期'] in missing_dates:
                                        data['kline'].append(item)
                                        added_count += 1

                                if added_count > 0:
                                    # 去重排序
                                    data['kline'] = sorted({item['交易日期']: item for item in data['kline']}.values(), key=lambda x: x['交易日期'])
                                    # 保存更新后的数据（使用DataSaver覆盖更新kline字段，不影响其他字段）
                                    DataSaver.save(data['kline'], json_path, format='json', mode='cover', field_name='kline', unique_key='交易日期')

                                    # 新增修复记录到报告
                                    report_item = {
                                        "股票代码": symbol,
                                        "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        "异常类型": "已修复",
                                        "异常日期": ",".join(missing_dates[:10]),
                                        "异常内容": f"K线自动修复完成，成功补全{added_count}条缺失数据",
                                        "状态": "已修复"
                                    }
                                    report_data.append(report_item)
                            except Exception:
                                if debug:
                                    import traceback
                                    traceback.print_exc()
                else:
                    report_item = {
                        "股票代码": symbol,
                        "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "异常类型": "K线数据缺失",
                        "异常日期": "",
                        "异常内容": "JSON文件缺少kline字段或K线数据为空",
                        "状态": "异常"
                    }
                    report_data.append(report_item)
                    fail_count += 1
                    all_valid = False

            # ------------------- 校验行情数据 -------------------
            if validate_type in ['all', 'quote']:
                if 'quote' in data and data['quote']:
                    valid, anomalies = validate_quote_data(data['quote'])
                    if not valid:
                        all_valid = False
                        for anomaly in anomalies:
                            report_item = {
                                "股票代码": symbol,
                                "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "异常类型": anomaly["异常类型"],
                                "异常日期": "",
                                "异常内容": anomaly["异常内容"],
                                "状态": "异常"
                            }
                            report_data.append(report_item)
                        fail_count += len(anomalies)
                else:
                    report_item = {
                        "股票代码": symbol,
                        "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "异常类型": "行情数据缺失",
                        "异常日期": "",
                        "异常内容": "JSON文件缺少quote字段或行情数据为空",
                        "状态": "异常"
                    }
                    report_data.append(report_item)
                    fail_count += 1
                    all_valid = False

            # ------------------- 校验财务数据 -------------------
            if validate_type in ['all', 'finance']:
                if 'finance' in data and data['finance']:
                    valid, anomalies = validate_finance_data(data['finance'])
                    if not valid:
                        all_valid = False
                        for anomaly in anomalies:
                            report_item = {
                                "股票代码": symbol,
                                "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "异常类型": anomaly["异常类型"],
                                "异常日期": anomaly.get("异常日期", ""),
                                "异常内容": anomaly["异常内容"],
                                "状态": "异常"
                            }
                            report_data.append(report_item)
                        fail_count += len(anomalies)
                else:
                    report_item = {
                        "股票代码": symbol,
                        "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "异常类型": "财务数据缺失",
                        "异常日期": "",
                        "异常内容": "JSON文件缺少finance字段或财务数据为空",
                        "状态": "异常"
                    }
                    report_data.append(report_item)
                    fail_count += 1
                    all_valid = False

            # ------------------- 校验资金流向数据 -------------------
            if validate_type in ['all', 'money_flow']:
                if 'money_flow' in data and data['money_flow']:
                    valid, anomalies = validate_moneyflow_data(data['money_flow'])
                    if not valid:
                        all_valid = False
                        for anomaly in anomalies:
                            report_item = {
                                "股票代码": symbol,
                                "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "异常类型": anomaly["异常类型"],
                                "异常日期": anomaly.get("异常日期", ""),
                                "异常内容": anomaly["异常内容"],
                                "状态": "异常"
                            }
                            report_data.append(report_item)
                        fail_count += len(anomalies)
                else:
                    report_item = {
                        "股票代码": symbol,
                        "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "异常类型": "资金流向数据缺失",
                        "异常日期": "",
                        "异常内容": "JSON文件缺少money_flow字段或资金流向数据为空",
                        "状态": "异常"
                    }
                    report_data.append(report_item)
                    fail_count += 1
                    all_valid = False

            # 全部校验通过
            if all_valid:
                report_item = {
                    "股票代码": symbol,
                    "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "异常类型": "无异常",
                    "异常日期": "",
                    "异常内容": f"{validate_type}类型数据质量校验通过",
                    "状态": "成功"
                }
                report_data.append(report_item)
                success_count += 1

        except Exception as e:
            report_item = {
                '股票代码': symbol,
                '校验时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                '异常类型': '校验失败',
                '异常日期': '',
                '异常内容': f'校验过程出错：{str(e)}',
                '状态': '失败'
            }
            report_data.append(report_item)
            fail_count += 1
            if debug:
                import traceback
                traceback.print_exc()

    # 写入质量报告
    write_quality_report(report_data)

    return report_data, success_count, fail_count


class DataSaver:
    """
    统一数据保存工具类，支持JSON/CSV/Excel三种格式，全量覆盖/增量追加两种模式
    自动处理单位转换、空值兼容、数据去重，完全兼容现有数据格式
    """

    @staticmethod
    def export_stock_json(symbols: list[str]) -> tuple[int, int, list[str]]:
        """导出单只/多只股票全量数据为JSON，每个股票一个单独文件保存在data目录
        :param symbols: 股票代码列表
        :return: 成功数量，失败数量，成功导出的文件路径列表
        """
        import json

        from .finance import FinanceFetcher
        from .money_flow import MoneyFlowFetcher
        from .quote import QuoteFetcher

        quote_fetcher = QuoteFetcher()
        finance_fetcher = FinanceFetcher()
        money_fetcher = MoneyFlowFetcher()
        success_count = 0
        failed_count = 0
        exported_files = []

        for code in symbols:
            try:
                # 拉取全量数据
                data = {}
                # 1. 实时行情
                quote_data = quote_fetcher.get_single_quote(code)
                data['quote'] = quote_data
                # 2. 财务报表
                finance_df = finance_fetcher.get_finance_report(code)
                data['finance'] = finance_df.to_dict(orient='records')
                # 3. 资金流向
                money_df = money_fetcher.get_stock_money_flow(code)
                data['money_flow'] = money_df.to_dict(orient='records')

                # 保存JSON到data根目录
                file_path = os.path.join("./data/", f"{code}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                exported_files.append(file_path)
                success_count += 1
            except Exception:
                failed_count += 1
                continue

        return success_count, failed_count, exported_files

    @staticmethod
    def _convert_unit(data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """
        内部方法：已完全删除单位转换逻辑，直接返回原始数据
        数据来源：雪球接口原始值，无任何修改
        """
        return data

    @staticmethod
    def _save_json(data: Union[Dict, List[Dict], pd.DataFrame], file_path: str, mode: str = "cover",
                  field_name: Optional[str] = None, unique_key: Optional[str] = None) -> bool:
        """
        内部方法：保存为JSON格式
        :param mode: cover-全量覆盖，append-增量追加
        :param field_name: 要保存到JSON中的字段名，必须传
        :param unique_key: 增量模式下的去重唯一键，比如交易日期/报告期/时间
        """
        if not field_name:
            raise ValueError("JSON格式保存必须指定field_name字段名")

        # DataFrame转字典
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient="records")

        # 单位转换
        data = DataSaver._convert_unit(data)

        # 读取现有文件内容（无论cover还是append模式，只要文件存在就先读取全部内容）
        existing_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                # 确保读取到的是字典类型，如果不是则重置为空字典
                if not isinstance(existing_data, dict):
                    existing_data = {}
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                # JSON文件损坏/格式错误/编码错误时，直接使用空字典，避免保存失败
                print(f"警告：文件{file_path}格式损坏，将覆盖原有内容")
                existing_data = {}

        # 全量覆盖模式直接替换字段
        if mode == "cover":
            existing_data[field_name] = data

        # 增量追加模式
        elif mode == "append":
            # 如果字段不存在直接新增
            if field_name not in existing_data:
                existing_data[field_name] = data
                existing_data[field_name].sort(key=lambda x: x.get(unique_key, ""), reverse=False)
            else:
                # 字段存在，按唯一键去重
                existing_items = existing_data[field_name]
                # 已存在的唯一键集合
                existing_keys = {item.get(unique_key, "") for item in existing_items if unique_key in item}
                # 添加新数据中不存在的项
                for item in data:
                    if unique_key in item and item[unique_key] not in existing_keys:
                        existing_items.append(item)
                # 按唯一键升序排序
                existing_items.sort(key=lambda x: x.get(unique_key, ""), reverse=False)
                existing_data[field_name] = existing_items

        # 保存文件
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2, default=str)
        return True

    @staticmethod
    def _save_csv(data: Union[Dict, List[Dict], pd.DataFrame], file_path: str) -> bool:
        """内部方法：保存为CSV格式"""
        # 字典转DataFrame
        if isinstance(data, (dict, list)):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        # 单位转换
        df = pd.DataFrame(DataSaver._convert_unit(df.to_dict(orient="records")))

        # 保存文件
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, index=False, encoding="utf_8_sig")
        return True

    @staticmethod
    def _save_excel(data: Union[Dict, List[Dict], pd.DataFrame], file_path: str) -> bool:
        """内部方法：保存为Excel格式"""
        # 字典转DataFrame
        if isinstance(data, (dict, list)):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        # 单位转换
        df = pd.DataFrame(DataSaver._convert_unit(df.to_dict(orient="records")))

        # 保存文件
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_excel(file_path, index=False)
        return True

    @staticmethod
    def save(data: Union[Dict, List[Dict], pd.DataFrame], file_path: str, format: str = "json",
             mode: str = "cover", field_name: Optional[str] = None, unique_key: Optional[str] = None) -> bool:
        """
        统一保存入口方法
        :param data: 要保存的数据，支持字典、列表、DataFrame
        :param file_path: 保存文件路径
        :param format: 保存格式：json/csv/excel
        :param mode: 保存模式：cover-全量覆盖，append-增量追加（仅JSON格式支持）
        :param field_name: JSON格式下要保存到的字段名
        :param unique_key: 增量模式下的去重唯一键
        :return: 保存成功返回True，失败/空数据返回False
        """
        # 空数据直接返回不保存
        if data is None or (isinstance(data, (list, pd.DataFrame)) and len(data) == 0):
            return False

        try:
            if format.lower() == "json":
                return DataSaver._save_json(data, file_path, mode, field_name, unique_key)
            elif format.lower() == "csv":
                return DataSaver._save_csv(data, file_path)
            elif format.lower() == "excel":
                return DataSaver._save_excel(data, file_path)
            else:
                raise ValueError(f"不支持的保存格式：{format}，仅支持json/csv/excel")
        except Exception as e:
            print(f"保存文件失败：{str(e)}")
            return False

    @staticmethod
    def load(file_path: str, format: str = "json") -> Union[Dict, pd.DataFrame, None]:
        """
        统一读取入口方法
        :param file_path: 要读取的文件路径
        :param format: 文件格式：json/csv/excel
        :return: 读取到的数据，失败返回None
        """
        if not os.path.exists(file_path):
            return None

        try:
            if format.lower() == "json":
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return data
                except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                    print(f"警告：文件{file_path}格式损坏，读取失败")
                    return None
            elif format.lower() == "csv":
                return pd.read_csv(file_path, encoding="utf_8_sig")
            elif format.lower() == "excel":
                return pd.read_excel(file_path)
            else:
                raise ValueError(f"不支持的读取格式：{format}，仅支持json/csv/excel")
        except Exception as e:
            print(f"读取文件失败：{str(e)}")
            return None




def validate_stock_code(symbol: str) -> tuple[bool, str]:
    """
    校验股票代码格式是否合法
    :param symbol: 股票代码，比如SZ000001
    :return: (是否合法, 错误信息)
    """
    if not symbol or not isinstance(symbol, str):
        return False, "股票代码不能为空"

    symbol = symbol.strip().upper()
    if not (symbol.startswith('SZ') or symbol.startswith('SH')):
        return False, "股票代码需要加前缀，格式为：SZ000001（深市）或SH600000（沪市）"

    code_part = symbol[2:]
    if not code_part.isdigit():
        return False, "股票代码前缀后必须是数字"

    if len(code_part) != 6:
        return False, "股票代码必须是6位数字（不含前缀）"

    # 排除北交所股票
    if code_part.startswith(('8', '4')):
        return False, "北交所股票暂不支持"

    # 排除科创板
    if code_part.startswith('68'):
        return False, "科创板股票暂不支持"

    return True, "格式合法"


def get_a_stock_trading_days(start_date: str, end_date: str) -> list[str]:
    """
    获取A股指定日期范围内的所有交易日，无需联网
    :param start_date: 开始日期，格式YYYY-MM-DD
    :param end_date: 结束日期，格式YYYY-MM-DD
    :return: 交易日列表，格式YYYY-MM-DD
    """
    try:
        import exchange_calendars as xcals
        # 获取上交所交易日历
        xshg = xcals.get_calendar("XSHG")
        # 转换日期范围
        schedule = xshg.schedule.loc[start_date:end_date]
        # 转换为字符串格式
        trading_days = schedule.index.strftime("%Y-%m-%d").tolist()
        return trading_days
    except Exception as e:
        print(f"获取交易日历失败：{str(e)}")
        return []


def validate_quote_data(quote_data: dict) -> tuple[bool, list[dict]]:
    """
    校验实时行情数据质量
    :param quote_data: 行情数据字典
    :return: (是否合格, 异常记录列表)
    """
    anomalies = []
    if not quote_data:
        anomalies.append({"异常类型": "无行情数据", "异常内容": "实时行情数据为空"})
        return False, anomalies

    required_fields = ["股票代码", "当前价格", "涨跌幅(%)", "成交量(手)", "成交额(万)"]
    for field in required_fields:
        if field not in quote_data or quote_data[field] is None:
            anomalies.append({
                "异常类型": "行情字段缺失",
                "异常内容": f"缺少必填字段：{field}"
            })

    # 校验数值合理性
    price = quote_data.get("当前价格")
    if price is not None and price <= 0:
        anomalies.append({
            "异常类型": "价格异常",
            "异常内容": f"当前价格{price}不合理，不能小于等于0"
        })

    pct_change = quote_data.get("涨跌幅(%)")
    if pct_change is not None and abs(pct_change) > 50:  # 最大涨跌幅限制50%（包含新股、ST、除权等极端情况）
        anomalies.append({
            "异常类型": "涨跌幅异常",
            "异常内容": f"涨跌幅{pct_change}%超出合理范围"
        })

    volume = quote_data.get("成交量(手)")
    if volume is not None and volume < 0:
        anomalies.append({
            "异常类型": "成交量异常",
            "异常内容": f"成交量{volume}为负数"
        })

    amount = quote_data.get("成交额(万)")
    if amount is not None and amount < 0:
        anomalies.append({
            "异常类型": "成交额异常",
            "异常内容": f"成交额{amount}为负数"
        })

    return len(anomalies) == 0, anomalies


def validate_finance_data(finance_data: list[dict]) -> tuple[bool, list[dict]]:
    """
    校验财务报表数据质量
    :param finance_data: 财务数据列表
    :return: (是否合格, 异常记录列表)
    """
    anomalies = []
    if not finance_data:
        anomalies.append({"异常类型": "无财务数据", "异常内容": "财务报表数据为空"})
        return False, anomalies

    for item in finance_data:
        report_period = item.get("报告期", "未知报告期")
        try:
            # 校验必填字段
            if "报告期" not in item or not item["报告期"]:
                anomalies.append({
                    "异常类型": "财务字段缺失",
                    "异常日期": report_period,
                    "异常内容": "缺少必填字段：报告期"
                })

            # 校验数值合理性
            net_profit = item.get("净利润(万)") or item.get("净利润(亿元)") or item.get("净利润同比(%)")
            if isinstance(net_profit, (int, float)) and abs(net_profit) > VALIDATION_THRESHOLD["max_numeric_value"]:  # 数值超过阈值
                anomalies.append({
                    "异常类型": "财务数据异常",
                    "异常日期": report_period,
                    "异常内容": f"净利润{net_profit}数值不合理"
                })

            # 百分比字段不能超过1000%
            pct_fields = ["净利润同比(%)", "毛利率(%)", "净利率(%)", "营收同比(%)"]
            for field in pct_fields:
                if field in item and isinstance(item[field], (int, float)):
                    if abs(item[field]) > VALIDATION_THRESHOLD["max_percent_value"]:
                        anomalies.append({
                            "异常类型": "财务百分比异常",
                            "异常日期": report_period,
                            "异常内容": f"{field}={item[field]}%超出合理范围"
                        })

        except Exception as e:
            anomalies.append({
                "异常类型": "财务数据解析异常",
                "异常日期": report_period,
                "异常内容": f"解析失败：{str(e)}"
            })

    return len(anomalies) == 0, anomalies


def validate_moneyflow_data(moneyflow_data: list[dict]) -> tuple[bool, list[dict]]:
    """
    校验资金流向数据质量
    :param moneyflow_data: 资金流向数据列表
    :return: (是否合格, 异常记录列表)
    """
    anomalies = []
    if not moneyflow_data:
        anomalies.append({"异常类型": "无资金流向数据", "异常内容": "资金流向数据为空"})
        return False, anomalies

    for item in moneyflow_data:
        time = item.get("时间", "未知时间")
        try:
            # 校验必填字段
            required_fields = ["时间"]
            for field in required_fields:
                if field not in item or not item[field]:
                    anomalies.append({
                        "异常类型": "资金流向字段缺失",
                        "异常日期": time,
                        "异常内容": f"缺少必填字段：{field}"
                    })

            # 校验数值合理性
            flow_fields = ["主力净流入(万元)", "散户净流入(万元)", "资金流向(万元)", "成交额(万)"]
            for field in flow_fields:
                if field in item and isinstance(item[field], (int, float)):
                    if abs(item[field]) > VALIDATION_THRESHOLD["max_numeric_value"]:  # 数值超过阈值
                        anomalies.append({
                            "异常类型": "资金流向数值异常",
                            "异常日期": time,
                            "异常内容": f"{field}={item[field]}数值不合理"
                        })

        except Exception as e:
            anomalies.append({
                "异常类型": "资金流向解析异常",
                "异常日期": time,
                "异常内容": f"解析失败：{str(e)}"
            })

    return len(anomalies) == 0, anomalies


def validate_kline_data(kline_data: list[dict], start_date: str, end_date: str) -> tuple[bool, list[dict], list[str]]:
    """
    校验历史K线数据质量
    :param kline_data: K线数据列表
    :param start_date: 校验开始日期
    :param end_date: 校验结束日期
    :return: (是否合格, 异常记录列表, 缺失日期列表)
    """
    if not kline_data:
        return False, [{"异常类型": "无K线数据", "异常日期": "", "异常内容": "无历史K线数据"}], []

    anomalies = []
    # 获取交易日列表
    trading_days = get_a_stock_trading_days(start_date, end_date)
    if not trading_days:
        return False, [{"异常类型": "交易日历获取失败", "异常日期": "", "异常内容": "无法获取A股交易日历"}], []

    # 现有数据的日期集合
    existing_dates = {item['交易日期'] for item in kline_data if '交易日期' in item}
    # 找出缺失的日期
    missing_dates = [d for d in trading_days if d not in existing_dates]

    if missing_dates:
        anomalies.append({
            "异常类型": "K线日期缺失",
            "异常日期": ",".join(missing_dates[:10]),  # 最多显示10个，太多截断
            "异常内容": f"共缺失{len(missing_dates)}个交易日数据"
        })

    # 校验每条数据的价格逻辑
    for item in kline_data:
        date = item.get('交易日期', '未知日期')
        try:
            open_p = item.get('开盘价')
            high_p = item.get('最高价')
            low_p = item.get('最低价')
            close_p = item.get('收盘价')
            volume = item.get('成交量(手)')
            amount = item.get('成交额(万)')
            pct_change = item.get('涨跌幅(%)')

            # 价格逻辑校验
            if None not in [open_p, high_p, low_p, close_p]:
                if not (low_p <= open_p <= high_p and low_p <= close_p <= high_p):
                    anomalies.append({
                        "异常类型": "价格逻辑异常",
                        "异常日期": date,
                        "异常内容": f"价格不符合逻辑：开盘={open_p},最高={high_p},最低={low_p},收盘={close_p}"
                    })

            # 涨跌幅合理性校验
            if pct_change is not None:
                name = item.get('股票名称', '')
                limit = VALIDATION_THRESHOLD["st_stock_limit"] if 'ST' in name else VALIDATION_THRESHOLD["normal_stock_limit"]
                if abs(pct_change) > limit * 100 * (1 + VALIDATION_THRESHOLD["pct_tolerance"]):  # 允许误差范围
                    anomalies.append({
                        "异常类型": "涨跌幅异常",
                        "异常日期": date,
                        "异常内容": f"涨跌幅{pct_change}%超出合理范围"
                    })

            # 成交量/成交额不能为负
            if volume is not None and volume < 0:
                anomalies.append({
                    "异常类型": "成交量异常",
                    "异常日期": date,
                    "异常内容": f"成交量{volume}为负数"
                })
            if amount is not None and amount < 0:
                anomalies.append({
                    "异常类型": "成交额异常",
                    "异常日期": date,
                    "异常内容": f"成交额{amount}为负数"
                })

        except Exception as e:
            anomalies.append({
                "异常类型": "数据解析异常",
                "异常日期": date,
                "异常内容": f"解析失败：{str(e)}"
            })

    return len(anomalies) == 0, anomalies, missing_dates


def write_quality_report(report_data: list[dict]):
    """
    追加写入数据质量报告到固定文件
    :param report_data: 报告记录列表
    """
    from datetime import datetime

    import pandas as pd

    report_path = PATH["quality_report_path"]
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 追加校验时间
    for item in report_data:
        item['校验时间'] = current_time

    df = pd.DataFrame(report_data)

    # 确保目录存在
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    # 追加写入，不存在则创建
    if os.path.exists(report_path):
        df.to_csv(report_path, mode='a', header=False, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(report_path, mode='w', header=True, index=False, encoding='utf-8-sig')

    print(f"[INFO] 质量报告已追加写入：{report_path}，本次共记录{len(report_data)}条异常")

