import os
import random
import time

import pysnowball as xq
import requests
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_exponential

from .config import ANTI_CRAWL

# 加载环境变量
load_dotenv()

class XueqiuClient:
    """雪球API客户端封装，包含完整防封策略"""
    _instance = None  # 单例模式

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_client()
        return cls._instance

    def _init_client(self):
        """初始化客户端配置"""
        # 读取配置
        self.cookie = os.getenv("XUEQIU_COOKIE")
        self.delay_min = ANTI_CRAWL["delay_min"]
        self.delay_max = ANTI_CRAWL["delay_max"]
        self.max_requests_per_minute = ANTI_CRAWL["max_requests_per_minute"]

        if not self.cookie:
            raise ValueError("请在.env文件中配置XUEQIU_COOKIE")

        # 设置雪球cookie
        xq.set_token(self.cookie)

        # 标记是否被封禁
        self.is_banned = False

    def _random_delay(self):
        """随机延迟1~2秒"""
        delay = random.uniform(self.delay_min, self.delay_max)
        time.sleep(delay)

    @sleep_and_retry
    @limits(calls=ANTI_CRAWL["max_requests_per_minute"], period=60)  # 每分钟最多请求次数
    def _rate_limit(self):
        """频率限制控制"""
        pass

    @staticmethod
    def _need_retry(result):
        """判断是否需要重试：返回空、或者包含5xx错误的需要重试"""
        if result is None:
            return True
        if isinstance(result, dict) and result.get("error_code") in ANTI_CRAWL["retry_error_codes"]:
            return True
        return False

    @retry(
        stop=stop_after_attempt(ANTI_CRAWL["max_retry_times"]),  # 最多重试次数
        wait=wait_exponential(multiplier=1, min=2, max=10),  # 重试间隔指数增长：2s/4s/8s
        retry=(
            retry_if_exception_type((
                requests.exceptions.RequestException,
                ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout
            ))
            | retry_if_result(_need_retry)
        ),
        reraise=True
    )
    def _call_api(self, func, *args, **kwargs):
        """调用API的统一入口，包含重试、延迟、错误处理"""
        if self.is_banned:
            raise Exception("❌ 检测到账号已被雪球封禁，停止所有请求，请更换Cookie或稍后再试")

        # 频率限制
        self._rate_limit()

        # 随机延迟
        self._random_delay()

        try:
            result = func(*args, **kwargs)

            # 检测是否被拦截
            if isinstance(result, dict):
                error_code = result.get("error_code")
                if error_code in ANTI_CRAWL["ban_error_codes"]:
                    self.is_banned = True
                    raise Exception(f"❌ 请求被雪球拦截，错误码：{error_code}，请重新获取Cookie或稍后再试")
                elif error_code in ANTI_CRAWL["retry_error_codes"]:
                    raise Exception(f"⚠️ 请求参数错误，错误码：{error_code}，请检查股票代码是否正确")

            # 空返回检查
            if result is None or (isinstance(result, dict) and not result.get("data")):
                raise Exception("⚠️ 接口返回空数据，请检查网络或Cookie是否有效")

            return result
        except Exception as e:
            error_msg = str(e)
            if "403" in error_msg or "Forbidden" in error_msg or "Unauthorized" in error_msg:
                self.is_banned = True
                raise Exception("❌ 检测到账号已被雪球封禁，请更换Cookie或稍后再试")
            elif "502" in error_msg or "503" in error_msg or "504" in error_msg:
                raise Exception(f"⚠️ 雪球服务端错误({error_msg[:3]})，正在重试...")
            elif "timeout" in error_msg.lower():
                raise Exception("⚠️ 请求超时，正在重试...")
            raise e

    # 以下是封装的API方法
    def get_quote(self, symbol):
        """获取股票实时行情"""
        return self._call_api(xq.realtime.quotec, symbol)

    def get_income(self, symbol):
        """获取利润表数据"""
        return self._call_api(xq.income, symbol)

    def get_balance(self, symbol):
        """获取资产负债表数据"""
        return self._call_api(xq.balance, symbol)

    def get_cash_flow(self, symbol):
        """获取现金流量表数据"""
        return self._call_api(xq.cash_flow, symbol)

    def get_money_flow_summary(self, symbol):
        """获取资金流向汇总数据"""
        return self._call_api(xq.moneyflow, symbol)

    def get_index_data(self, index_symbol):
        """获取指数数据"""
        return self._call_api(xq.quote, index_symbol)

    def get_industry_plate(self):
        """获取行业板块数据"""
        return self._call_api(xq.industry)

    def get_stock_announcement(self, symbol, count=10):
        """获取公司公告"""
        return self._call_api(xq.announcement, symbol, count)

    def get_lhb_data(self, date=None):
        """获取龙虎榜数据"""
        if date:
            return self._call_api(xq.lhb, date)
        return self._call_api(xq.lhb)

    def get_north_money(self):
        """获取北向资金数据"""
        return self._call_api(xq.northmoney)

    def get_fund_net_value(self, fund_code):
        """获取基金净值数据"""
        return self._call_api(xq.fund_nav, fund_code)

    def get_money_flow_minute(self, symbol):
        """获取个股资金流向分钟级数据"""
        return self._call_api(xq.capital_flow, symbol)

    def get_kline(self, symbol, period='day', count=1):
        """获取K线数据
        :param symbol: 股票代码
        :param period: 周期：day/week/month
        :param count: 获取数量
        """
        return self._call_api(xq.kline, symbol, period, count)
