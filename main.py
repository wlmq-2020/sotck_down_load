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
from stock_download.utils import DataSaver, check_root_dir_py_files

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
    from stock_download.task import fill_history_kline
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
    from stock_download.task import filter_stocks, daily_task, weekly_task, monthly_task
    try:
        click.echo("正在拉取全市场股票数据...")
        filtered, output_path = filter_stocks()
        click.echo(f"\n筛选完成，共找到{len(filtered)}只符合条件的股票")
        click.echo(f"已更新到{output_path}")

        # 如果带--update参数，自动执行全量更新任务
        if update:
            click.echo("\n开始自动执行全量数据更新...")
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
    except Exception as e:
        click.echo(f"❌ 筛选股票失败：{str(e)}", err=True)

# --------------- 导出单股票全量JSON命令 ---------------
@main.command(name="export-json", help="导出单只/多只股票全量数据为JSON，每个股票一个单独文件保存在data目录\n参数：symbols 股票代码，多个用空格分隔，例如：SZ000001 SH600000")
@click.argument("symbols", nargs=-1, required=True)
def export_json_cmd(symbols):
    from stock_download.utils import DataSaver
    try:
        success_count, failed_count, exported_files = DataSaver.export_stock_json(symbols)
        for file_path in exported_files:
            click.echo(f"已导出：{file_path}")
        click.echo(f"\n导出完成，成功{success_count}只，失败{failed_count}只")
    except Exception as e:
        click.echo(f"❌ 导出JSON失败：{str(e)}", err=True)

# --------------- 初始化命令 ---------------
@main.command(name="init", help="初始化项目，创建所需目录、生成.env模板")
@click.pass_context
def init_cmd(ctx):
    from stock_download.utils import init_project
    try:
        created_dirs, env_template_path = init_project()
        for d in created_dirs:
            click.info(f"创建目录：{d}")
        if env_template_path:
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
    except Exception as e:
        click.echo(f"❌ 初始化项目失败：{str(e)}", err=True)


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

# --------------- 自选股一键更新命令 ---------------
@main.command(name="quote-update", help="一键更新所有自选股票的全量数据，不需要任何参数\n自动读取 ./data/自选股票列表.csv 中的股票，更新实时行情、历史K线、财务数据、资金流向")
@click.pass_context
def custom_update_cmd(ctx):
    from stock_download.task import update_custom_stocks
    try:
        update_custom_stocks()
    except Exception as e:
        click.echo(f"自选股更新失败：{str(e)}", err=True)
        if ctx.obj['DEBUG']:
            import traceback
            traceback.print_exc()

# --------------- 自选股历史K线增量补全命令 ---------------

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
    import pandas as pd
    from stock_download.utils import validate_stock_data, validate_stock_code

    try:
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

        # 调用校验方法
        report_data, success_count, fail_count = validate_stock_data(
            symbols, validate_type=type, auto_fix=auto_fix,
            start_date=start_date, end_date=end_date, debug=ctx.obj['DEBUG']
        )

        # 输出统计结果
        click.secho("\n=== 数据质量校验完成 ===", fg='green', bold=True)
        click.echo(f"校验股票总数：{len(symbols)}")
        click.echo(f"校验数据类型：{type}")
        click.echo(f"校验通过：{success_count}只")
        click.echo(f"发现异常：{fail_count}条")
        click.echo("报告已写入：./data/index/data_quality_report.csv")
    except Exception as e:
        click.echo(f"❌ 数据校验失败：{str(e)}", err=True)
        if ctx.obj['DEBUG']:
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
