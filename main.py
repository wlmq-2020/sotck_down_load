#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
项目统一入口，所有数据下载功能必须通过本文件调用
使用方法：python main.py [命令] [参数]
"""
import os

import click
from dotenv import load_dotenv

# 加载环境变量（必须放在最前面）
load_dotenv()
# 导入目录检测工具和统一数据保存工具
from stock_download.utils import DataSaver, check_root_dir_py_files, validate_kline_data, write_quality_report

VERSION = "1.0.0"

@click.group(help="基于pysnowball的A股数据获取工具，所有下载功能统一入口", context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--debug", "-d", is_flag=True, help="开启调试模式，打印详细日志")
@click.version_option(VERSION, '-v', '--version', prog_name="A股数据获取工具")
@click.pass_context
def main(ctx, debug):

    # 保存全局参数到context
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug
    ctx.obj['VERSION'] = VERSION

    # 执行前先检查目录规范，不符合直接退出
    check_root_dir_py_files()

    # 设置click输出样式
    click.debug = lambda msg: click.secho(f"[DEBUG] {msg}", fg='blue', dim=True) if debug else None
    click.info = lambda msg: click.secho(f"[INFO] {msg}", fg='green')
    click.warning = lambda msg: click.secho(f"[WARNING] {msg}", fg='yellow')
    click.error = lambda msg: click.secho(f"[ERROR] {msg}", fg='red', err=True)

# --------------- 实时行情命令 ---------------
@main.command(name="quote", help="获取股票实时行情数据\n参数：symbols 股票代码，多个用空格分隔，例如：SZ000001 SH600000")
@click.argument("symbols", nargs=-1, required=True)
@click.option("--output", "-o", help="输出文件路径，支持.csv/.xlsx/.json，例如：./data/quote.csv")
@click.pass_context
def quote_cmd(ctx, symbols, output):
    from stock_download.quote import QuoteFetcher
    from stock_download.utils import validate_stock_code
    try:
        # 校验所有股票代码
        valid_symbols = []
        for symbol in symbols:
            valid, msg = validate_stock_code(symbol)
            if not valid:
                click.warning(f"跳过无效股票代码{symbol}：{msg}")
                continue
            valid_symbols.append(symbol)

        if not valid_symbols:
            click.error("没有有效的股票代码，请检查输入")
            return

        fetcher = QuoteFetcher()
        if len(valid_symbols) == 1:
            data = fetcher.get_single_quote(valid_symbols[0])
            click.secho("\n 实时行情数据：", fg='green', bold=True)
            for k, v in data.items():
                if isinstance(v, float):
                    v = round(v, 2)
                click.echo(f"{k}: {v}")
            if output:
                DataSaver.save(data, output)
                click.info(f"数据已保存到：{output}")
        else:
            df = fetcher.get_batch_quotes(valid_symbols)
            click.secho("\n 实时行情数据：", fg='green', bold=True)
            click.echo(df.to_string(index=False))
            if output:
                DataSaver.save(df, output)
                click.info(f"数据已保存到：{output}")
    except Exception as e:
        click.error(f"获取行情失败：{str(e)}")
        if ctx.obj['DEBUG']:
            import traceback
            traceback.print_exc()

# --------------- 财务数据命令 ---------------
@main.command(name="finance", help="获取财务基本面数据\n参数：symbol 股票代码，例如：SZ000001")
@click.argument("symbol", required=True)
@click.option("--report-type", "-t", default="all", help="报表类型：all(全部), income(利润表), balance(资产负债表), cash(现金流量表)")
@click.option("--output", "-o", help="输出文件路径")
def finance_cmd(symbol, report_type, output):
    from stock_download.finance import FinanceFetcher
    try:
        fetcher = FinanceFetcher()
        df = fetcher.get_finance_report(symbol, report_type)
        click.echo("\n财务报表数据：")
        click.echo(df.to_string(index=False))
        if output:
            DataSaver.save(df, output)
            click.echo(f"\n✅ 数据已保存到：{output}")
    except Exception as e:
        click.echo(f"❌ 获取财务数据失败：{str(e)}", err=True)

# --------------- 资金流向命令 ---------------
@main.command(name="money-flow", help="获取资金流向数据")
@click.option("--symbol", "-s", help="股票代码，不传默认获取北向资金和龙虎榜")
@click.option("--lhb", is_flag=True, help="获取龙虎榜数据")
@click.option("--date", "-d", help="龙虎榜日期，格式YYYY-MM-DD")
@click.option("--output", "-o", help="输出文件路径")
def money_flow_cmd(symbol, lhb, date, output):
    from stock_download.money_flow import MoneyFlowFetcher
    try:
        fetcher = MoneyFlowFetcher()
        if symbol:
            df = fetcher.get_stock_money_flow(symbol)
            click.echo(f"\n{symbol} 今日资金流向分钟级数据：")
            # 只显示最近20条+汇总
            if len(df) > 20:
                click.echo(df.tail(21).to_string(index=False))
                click.echo(f"\n... 共{len(df)-1}条分钟级数据，已省略历史部分")
            else:
                click.echo(df.to_string(index=False))

            if output:
                DataSaver.save(df, output)
                click.echo(f"\n✅ 完整数据已保存到：{output}")
        elif lhb:
            df = fetcher.get_lhb_data(date)
            click.echo("\n龙虎榜数据：")
            click.echo(df.to_string(index=False))
            if output:
                DataSaver.save(df, output)
                click.echo(f"\n✅ 数据已保存到：{output}")
        else:
            # 暂时屏蔽北向资金接口，待适配
            click.echo("ℹ️ 北向资金接口正在适配中，当前支持查询个股资金流向：python main.py money-flow --symbol SZ000001")
    except Exception as e:
        click.echo(f"❌ 获取资金流向失败：{str(e)}", err=True)

# --------------- 指数基金命令 ---------------
@main.command(name="index", help="获取指数与基金数据")
@click.option("--major", is_flag=True, help="获取主要大盘指数行情")
@click.option("--industry", is_flag=True, help="获取行业板块数据")
@click.option("--fund", "-f", help="基金代码，获取基金净值数据")
@click.option("--etf", "-e", help="ETF代码，获取ETF实时行情")
@click.option("--output", "-o", help="输出文件路径")
def index_cmd(major, industry, fund, etf, output):
    from stock_download.index_fund import IndexFundFetcher
    try:
        fetcher = IndexFundFetcher()
        if major:
            df = fetcher.get_major_index_quotes()
            click.echo("\n主要大盘指数行情：")
            click.echo(df.to_string(index=False))
            if output:
                DataSaver.save(df, output)
                click.echo(f"\n✅ 数据已保存到：{output}")
        elif industry:
            click.echo("ℹ️ 行业板块接口正在适配中")
        elif fund:
            click.echo("ℹ️ 基金净值接口正在适配中")
        elif etf:
            click.echo("ℹ️ ETF行情接口正在适配中")
        else:
            click.echo("请指定查询类型：--major(大盘指数)")
    except Exception as e:
        click.echo(f"❌ 获取指数基金数据失败：{str(e)}", err=True)

# --------------- 深度资料命令 ---------------
@main.command(name="deep", help="获取深度资料数据\n参数：symbol 股票代码")
@click.argument("symbol", required=True)
@click.option("--announcement", "-a", is_flag=True, help="获取公司公告")
@click.option("--count", "-c", default=10, help="公告数量，默认10条")
@click.option("--margin", "-m", is_flag=True, help="获取融资融券数据")
@click.option("--output", "-o", help="输出文件路径")
def deep_cmd(symbol, announcement, count, margin, output):
    try:
        if announcement:
            click.echo("ℹ️ 公司公告接口正在适配中")
        elif margin:
            click.echo("ℹ️ 融资融券接口正在适配中")
        else:
            click.echo("请指定查询类型：--announcement(公司公告) / --margin(融资融券)")
    except Exception as e:
        click.echo(f"❌ 获取深度资料失败：{str(e)}", err=True)

# --------------- 历史数据补全命令 ---------------
@main.command(name="fill-history", help="补全股票5年历史K线数据\n参数：--days 补全天数，默认1825天（5年）")
@click.option("--days", default=1825, help="补全历史数据天数")
def fill_history_cmd(days):
    from src.stock_download.task import fill_history_kline
    try:
        fill_history_kline(days)
    except Exception as e:
        click.echo(f"❌ 历史数据补全失败：{str(e)}", err=True)

# --------------- 批量爬取任务命令 ---------------
@main.command(name="task", help="手动运行分层爬取任务\n参数：--type 任务类型：daily(日级)/weekly(周级)/monthly(月级)")
@click.option("--type", "-t", required=True, type=click.Choice(['daily', 'weekly', 'monthly']), help="任务类型")
def task_cmd(type):
    from stock_download.task import daily_task, monthly_task, weekly_task
    try:
        if type == 'daily':
            daily_task()
        elif type == 'weekly':
            weekly_task()
        elif type == 'monthly':
            monthly_task()
    except Exception as e:
        click.echo(f"❌ 任务执行失败：{str(e)}", err=True)

# --------------- 定时调度服务命令 ---------------
@main.command(name="schedule", help="启动定时调度服务，自动按周期运行爬取任务")
def schedule_cmd():
    from stock_download.task import start_schedule
    try:
        start_schedule()
    except KeyboardInterrupt:
        click.echo("\n✅ 调度服务已停止")
    except Exception as e:
        click.echo(f"调度服务启动失败：{str(e)}", err=True)

# --------------- 筛选股票命令 ---------------
@main.command(name="filter-stocks", help="自动筛选沪深主板非ST、市值50-300亿的股票，更新到股票列表.csv")
@click.option("--update", is_flag=True, help="筛选完成后自动执行全量更新任务")
def filter_stocks_cmd(update):
    import os

    import pandas as pd
    import requests
    from dotenv import load_dotenv

    load_dotenv()
    cookie = os.getenv("XUEQIU_COOKIE")
    headers = {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://xueqiu.com/hq/screener",
        "X-Requested-With": "XMLHttpRequest"
    }

    all_stocks = []
    page = 1
    size = 200

    click.echo("正在拉取全市场股票数据...")
    # 分页拉取所有沪深A股
    while True:
        url = f"https://stock.xueqiu.com/v5/stock/screener/quote/list.json?page={page}&size={size}&order=desc&orderby=market_capital&market=CN&type=sh_sz&_=1712990000000"
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            stock_list = data.get('data', {}).get('list', [])
            if not stock_list:
                break
            all_stocks.extend(stock_list)
            click.echo(f"已拉取第{page}页，共{len(all_stocks)}只股票")
            if len(stock_list) < size:
                break
            page += 1
        except Exception as e:
            click.echo(f"拉取第{page}页失败：{str(e)}", err=True)
            break

    # 筛选条件
    filtered = []
    for stock in all_stocks:
        symbol = stock['symbol']
        name = stock['name']
        # 1. 排除ST/*ST股票
        if 'ST' in name or '*ST' in name:
            continue
        # 2. 保留上证主板(SH60开头)、深证主板(SZ00开头)、创业板(SZ30开头)，排除科创板、北交所
        if not (symbol.startswith('SH60') or symbol.startswith('SZ00') or symbol.startswith('SZ30')):
            continue
        # 3. 总市值在50-300亿之间（接口返回单位为元，转成亿）
        market_cap = stock.get('market_capital', 0) / 100000000
        if not (50 <= market_cap <= 300):
            continue
        # 格式化写入格式
        code = symbol[2:]  # 去掉前缀，比如SZ000422 -> 000422
        filtered.append({
            'code': code,
            'name': name,
            'full_code': symbol
        })

    click.echo(f"\n筛选完成，共找到{len(filtered)}只符合条件的股票")

    if len(filtered) == 0:
        click.echo("没有找到符合条件的股票，不更新股票列表", err=True)
        return

    # 写入股票列表.csv
    df = pd.DataFrame(filtered)
    output_path = './data/股票列表.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    click.echo(f"已更新到{output_path}")

    # 如果带--update参数，自动执行全量更新任务
    if update:
        click.echo("\n开始自动执行全量数据更新...")
        from stock_download.task import daily_task, monthly_task, weekly_task
        try:
            click.echo("=== 执行日级任务 ===")
            daily_task()
            click.echo("\n=== 执行周级任务 ===")
            weekly_task()
            click.echo("\n=== 执行月级任务 ===")
            monthly_task()
            click.echo("\n✅ 全量数据更新完成")
        except Exception as e:
            click.echo(f"❌ 更新失败：{str(e)}", err=True)

# --------------- 导出单股票全量JSON命令 ---------------
@main.command(name="export-json", help="导出单只/多只股票全量数据为JSON，每个股票一个单独文件保存在data目录\n参数：symbols 股票代码，多个用空格分隔，例如：SZ000001 SH600000")
@click.argument("symbols", nargs=-1, required=True)
def export_json_cmd(symbols):
    import json
    import os

    from stock_download.finance import FinanceFetcher
    from stock_download.money_flow import MoneyFlowFetcher
    from stock_download.quote import QuoteFetcher
    quote_fetcher = QuoteFetcher()
    finance_fetcher = FinanceFetcher()
    money_fetcher = MoneyFlowFetcher()
    success_count = 0
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
            click.echo(f"已导出：{file_path}")
            success_count += 1
        except Exception as e:
            click.echo(f"导出{code}失败：{str(e)}", err=True)
    click.echo(f"\n导出完成，成功{success_count}只，失败{len(symbols)-success_count}只")

# --------------- 初始化命令 ---------------
@main.command(name="init", help="初始化项目，创建所需目录、生成.env模板")
@click.pass_context
def init_cmd(ctx):
    # 创建需要的目录
    dirs = [
        "./data",
        "./logs",
        "./temp",
        "./tests",
        "./data/index"
    ]
    for d in dirs:
        if not os.path.exists(d):
            os.makedirs(d)
            click.info(f"创建目录：{d}")
        else:
            click.debug(f"目录已存在：{d}")

    # 生成.env模板
    env_template_path = "./.env.template"
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
        click.info("已生成.env.template模板文件，请复制为.env并配置Cookie")
    else:
        click.debug(".env.template已存在，跳过生成")

    # 检查Cookie配置
    cookie = os.getenv("XUEQIU_COOKIE")
    if not cookie or cookie == "your_xueqiu_cookie_here":
        click.warning("请在.env文件中配置XUEQIU_COOKIE后再使用其他功能")
        click.info("提示：Cookie获取教程：浏览器打开xueqiu.com登录后，按F12打开开发者工具，在Network标签页随便找个请求，复制Request Headers里的Cookie值即可")
    else:
        click.info(".env配置检查完成")

    click.info("项目初始化完成，现在可以开始使用了！")
    click.info("提示：首次使用建议先运行：python main.py filter-stocks 生成股票列表")


# --------------- 数据预览命令 ---------------
@main.command(name="preview", help="预览股票数据，不用打开JSON文件直接在控制台查看")
@click.argument("symbol", required=True)
@click.option("--type", "-t", required=True, type=click.Choice(['quote', 'history', 'finance', 'money', 'all']), help="要预览的数据类型：quote(行情)/history(历史K线)/finance(财务)/money(资金流向)/all(全部)")
@click.option("--days", "-n", default=10, help="预览历史K线/资金流向的天数，默认10天")
@click.pass_context
def preview_cmd(ctx, symbol, type, days):
    from stock_download.finance import FinanceFetcher
    from stock_download.money_flow import MoneyFlowFetcher
    from stock_download.quote import QuoteFetcher
    from stock_download.utils import validate_stock_code

    # 校验股票代码
    valid, msg = validate_stock_code(symbol)
    if not valid:
        click.error(msg)
        return

    click.info(f"正在预览{symbol}的{type}数据...")

    try:
        if type in ['quote', 'all']:
            fetcher = QuoteFetcher()
            quote_data = fetcher.get_single_quote(symbol)
            click.secho("\n 实时行情：", fg='cyan', bold=True)
            for k, v in quote_data.items():
                if isinstance(v, float):
                    v = round(v, 2)
                click.echo(f"{k}: {v}")

        if type in ['history', 'all']:
            fetcher = QuoteFetcher()
            df = fetcher.get_history_kline(symbol, days)
            click.secho(f"\n 近{days}天历史K线：", fg='cyan', bold=True)
            click.echo(df.to_string(index=False))

        if type in ['finance', 'all']:
            fetcher = FinanceFetcher()
            df = fetcher.get_finance_report(symbol)
            click.secho("\n 财务报表：", fg='cyan', bold=True)
            click.echo(df.to_string(index=False))

        if type in ['money', 'all']:
            fetcher = MoneyFlowFetcher()
            df = fetcher.get_stock_money_flow(symbol)
            click.secho("\n 今日资金流向：", fg='cyan', bold=True)
            click.echo(df.to_string(index=False))

    except Exception as e:
        click.error(f"预览失败：{str(e)}")
        if ctx.obj['DEBUG']:
            import traceback
            traceback.print_exc()

# --------------- 数据质量校验命令 ---------------
@main.command(name="validate-data", help="校验已下载的股票数据质量，生成数据质量报告\n默认校验所有股票所有类型数据，也可指定股票或校验类型")
@click.argument("symbols", nargs=-1, required=False)
@click.option("--type", "-t", default="all", type=click.Choice(['all', 'kline', 'quote', 'finance', 'money_flow']),
              help="校验数据类型：all(全部)/kline(K线)/quote(行情)/finance(财务)/money_flow(资金流向)，默认all")
@click.option("--auto-fix", "-f", is_flag=True, help="自动修复缺失的K线数据，重新下载缺失日期")
@click.option("--start-date", "-s", default="2021-01-01", help="K线校验开始日期，默认2021-01-01")
@click.option("--end-date", "-e", default=None, help="K线校验结束日期，默认今日")
@click.pass_context
def validate_data_cmd(ctx, symbols, type, auto_fix, start_date, end_date):
    from datetime import datetime

    import pandas as pd

    from stock_download.quote import QuoteFetcher
    from stock_download.utils import (
        validate_finance_data,
        validate_moneyflow_data,
        validate_quote_data,
        validate_stock_code,
    )

    # 设置默认结束日期为今日
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # 如果没有指定股票，读取股票列表
    if not symbols:
        stock_list_path = './data/股票列表.csv'
        if not os.path.exists(stock_list_path):
            click.error(f"股票列表文件不存在：{stock_list_path}，请先运行 python main.py filter-stocks 生成")
            return
        df_stocks = pd.read_csv(stock_list_path, dtype={'code': str, 'full_code': str})
        symbols = df_stocks['full_code'].tolist()
        click.info(f"读取到{len(symbols)}只股票，开始批量校验...")
    else:
        # 校验输入的股票代码
        valid_symbols = []
        for symbol in symbols:
            valid, msg = validate_stock_code(symbol)
            if not valid:
                click.warning(f"跳过无效股票代码{symbol}：{msg}")
                continue
            valid_symbols.append(symbol)
        symbols = valid_symbols
        if not symbols:
            click.error("没有有效的股票代码，请检查输入")
            return
        click.info(f"开始校验{len(symbols)}只股票的{type}类型数据质量...")

    report_data = []
    success_count = 0
    fail_count = 0

    fetcher = QuoteFetcher()

    for symbol in symbols:
        click.debug(f"正在校验{symbol}...")
        try:
            # 读取已下载的K线数据
            json_path = f"./data/{symbol}.json"
            if not os.path.exists(json_path):
                report_item = {
                    '股票代码': symbol,
                    '校验时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    '异常类型': '文件不存在',
                    '异常日期': '',
                    '异常内容': f'JSON数据文件不存在：{json_path}',
                    '状态': '失败'
                }
                report_data.append(report_item)
                fail_count += 1
                click.warning(f"{symbol} 数据文件不存在，跳过")
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
                click.warning(f"{symbol} JSON文件为空，跳过")
                continue

            # 初始化所有校验结果
            all_valid = True

            # ------------------- 校验K线数据 -------------------
            if type in ['all', 'kline']:
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
                        click.warning(f"{symbol} K线数据发现{len(anomalies)}个异常，缺失{len(missing_dates)}个交易日数据")

                        # K线自动修复
                        if auto_fix and missing_dates:
                            click.info(f"开始自动修复{symbol}的缺失K线数据，共{len(missing_dates)}个交易日...")
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
                                    # 保存更新后的数据
                                    import json
                                    os.makedirs(os.path.dirname(json_path), exist_ok=True)
                                    with open(json_path, 'w', encoding='utf-8') as f:
                                        json.dump(data, f, ensure_ascii=False, indent=2)
                                    click.info(f"{symbol} K线修复完成，成功补全{added_count}条数据")

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
                                else:
                                    click.warning(f"{symbol} K线修复失败，接口未返回任何缺失日期的数据")
                            except Exception as e:
                                click.error(f"{symbol} K线自动修复失败：{str(e)}")
                                if ctx.obj['DEBUG']:
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
                    click.warning(f"{symbol} 缺少K线数据")

            # ------------------- 校验行情数据 -------------------
            if type in ['all', 'quote']:
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
                        click.warning(f"{symbol} 行情数据发现{len(anomalies)}个异常")
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
                    click.warning(f"{symbol} 缺少行情数据")

            # ------------------- 校验财务数据 -------------------
            if type in ['all', 'finance']:
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
                        click.warning(f"{symbol} 财务数据发现{len(anomalies)}个异常")
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
                    click.warning(f"{symbol} 缺少财务数据")

            # ------------------- 校验资金流向数据 -------------------
            if type in ['all', 'money_flow']:
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
                        click.warning(f"{symbol} 资金流向数据发现{len(anomalies)}个异常")
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
                    click.warning(f"{symbol} 缺少资金流向数据")

            # 全部校验通过
            if all_valid:
                report_item = {
                    "股票代码": symbol,
                    "校验时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "异常类型": "无异常",
                    "异常日期": "",
                    "异常内容": f"{type}类型数据质量校验通过",
                    "状态": "成功"
                }
                report_data.append(report_item)
                success_count += 1
                click.debug(f"{symbol} 校验通过")

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
            click.error(f"{symbol} 校验失败：{str(e)}")
            if ctx.obj['DEBUG']:
                import traceback
                traceback.print_exc()

    # 写入质量报告
    write_quality_report(report_data)

    # 输出统计结果
    click.secho("\n=== 数据质量校验完成 ===", fg='green', bold=True)
    click.echo(f"校验股票总数：{len(symbols)}")
    click.echo(f"校验数据类型：{type}")
    click.echo(f"校验通过：{success_count}只")
    click.echo(f"发现异常：{fail_count}条")
    click.echo("报告已写入：./data/index/data_quality_report.csv")


if __name__ == "__main__":
    main()
