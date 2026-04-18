#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
项目统一入口，所有数据下载功能必须通过本文件调用
使用方法：python main.py [命令] [参数]
"""
import click
from dotenv import load_dotenv

# 加载环境变量（必须放在最前面）
load_dotenv()

@click.group(help="基于pysnowball的A股数据获取工具，所有下载功能统一入口")
def main():
    pass

# --------------- 实时行情命令 ---------------
@main.command(name="quote", help="获取股票实时行情数据\n参数：symbols 股票代码，多个用空格分隔，例如：SZ000001 SH600000")
@click.argument("symbols", nargs=-1, required=True)
@click.option("--output", "-o", help="输出文件路径，支持.csv/.xlsx/.json，例如：./data/quote.csv")
def quote_cmd(symbols, output):
    from stock_download.quote import QuoteFetcher
    try:
        fetcher = QuoteFetcher()
        if len(symbols) == 1:
            data = fetcher.get_single_quote(symbols[0])
            click.echo("\n实时行情数据：")
            for k, v in data.items():
                click.echo(f"{k}: {v}")
            if output:
                fetcher.save_quote(data, output)
                click.echo(f"\n✅ 数据已保存到：{output}")
        else:
            df = fetcher.get_batch_quotes(list(symbols))
            click.echo("\n实时行情数据：")
            click.echo(df.to_string(index=False))
            if output:
                fetcher.save_quote(df, output)
                click.echo(f"\n✅ 数据已保存到：{output}")
    except Exception as e:
        click.echo(f"❌ 获取行情失败：{str(e)}", err=True)

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
            fetcher.save_finance_data(df, output)
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
                fetcher.save_money_flow_data(df, output)
                click.echo(f"\n✅ 完整数据已保存到：{output}")
        elif lhb:
            df = fetcher.get_lhb_data(date)
            click.echo("\n龙虎榜数据：")
            click.echo(df.to_string(index=False))
            if output:
                fetcher.save_money_flow_data(df, output)
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
                fetcher.save_index_fund_data(df, output)
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

# --------------- 批量爬取任务命令 ---------------
@main.command(name="task", help="手动运行分层爬取任务\n参数：--type 任务类型：daily(日级)/weekly(周级)/monthly(月级)")
@click.option("--type", "-t", required=True, type=click.Choice(['daily', 'weekly', 'monthly']), help="任务类型")
def task_cmd(type):
    from stock_download.task import daily_task, weekly_task, monthly_task
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
    import pandas as pd
    import requests
    from dotenv import load_dotenv
    import os

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
        # 2. 只保留上证主板(SH60开头)和深证主板(SZ00开头)，排除创业板、科创板、北交所
        if not (symbol.startswith('SH60') or symbol.startswith('SZ00')):
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
        from stock_download.task import daily_task, weekly_task, monthly_task
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
    from stock_download.quote import QuoteFetcher
    from stock_download.finance import FinanceFetcher
    from stock_download.money_flow import MoneyFlowFetcher
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

            # ---------------- 新增：启动信号核心指标 ----------------
            signal = {}
            # 1. 量价信号
            signal['当日量比'] = round(quote_data['成交量(手)'] / (quote_data.get('5日均量', quote_data['成交量(手)'])), 2)
            signal['近5日涨跌幅(%)'] = quote_data.get('近5日涨跌幅', 0)
            signal['近20日涨跌幅(%)'] = quote_data.get('近20日涨跌幅', 0)
            signal['当日振幅(%)'] = quote_data['振幅(%)']

            # 2. 资金信号
            total_flow = money_df[money_df['时间'] != '今日汇总']['资金流向(万元)'].sum()
            signal['当日主力资金净流入(万元)'] = round(total_flow, 2)

            # 3. 基本面信号
            if len(finance_df) > 0:
                latest_finance = finance_df.iloc[0]
                signal['最新季度净利润同比(%)'] = latest_finance['净利润同比(%)']
                signal['动态市盈率(TTM)'] = quote_data.get('市盈率(TTM)', 0)

            # 4. 计算启动概率分（0-10分）
            score = 0
            if signal['当日量比'] > 1.5: score += 2  # 放量加2分
            if signal['当日主力资金净流入(万元)'] > 2000: score += 2  # 资金流入加2分
            if signal['最新季度净利润同比(%)'] > 20: score += 2  # 业绩增长加2分
            if signal['近5日涨跌幅(%)'] > 5 and signal['近5日涨跌幅(%)'] < 20: score += 2  # 温和上涨加2分
            if signal['动态市盈率(TTM)'] > 0 and signal['动态市盈率(TTM)'] < 30: score += 1  # 低估值加1分
            if quote_data['换手率(%)'] > 3 and quote_data['换手率(%)'] < 15: score +=1  # 健康换手加1分
            signal['启动概率分(0-10)'] = min(score, 10)

            data['start_signal'] = signal
            # 保存JSON到data根目录
            file_path = os.path.join("./data/", f"{code}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            click.echo(f"已导出：{file_path}")
            success_count += 1
        except Exception as e:
            click.echo(f"导出{code}失败：{str(e)}", err=True)
    click.echo(f"\n导出完成，成功{success_count}只，失败{len(symbols)-success_count}只")

if __name__ == "__main__":
    main()
