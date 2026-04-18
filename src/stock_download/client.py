import os
import random
import time

import pysnowball as xq
import requests
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

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
        self.delay_min = float(os.getenv("REQUEST_DELAY_MIN", 1))
        self.delay_max = float(os.getenv("REQUEST_DELAY_MAX", 2))
        self.max_requests_per_minute = int(os.getenv("MAX_REQUESTS_PER_MINUTE", 30))

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
    @limits(calls=30, period=60)  # 每分钟最多30次请求
    def _rate_limit(self):
        """频率限制控制"""
        pass

    @retry(
        stop=stop_after_attempt(1),  # 最多重试1次（符合要求）
        wait=wait_exponential(multiplier=1, min=2, max=10),  # 重试间隔指数增长
        retry=retry_if_exception_type((requests.exceptions.RequestException, ConnectionError))
    )
    def _call_api(self, func, *args, **kwargs):
        """调用API的统一入口，包含重试、延迟、错误处理"""
        if self.is_banned:
            raise Exception("检测到账号已被雪球封禁，停止所有请求")

        # 频率限制
        self._rate_limit()

        # 随机延迟
        self._random_delay()

        try:
            result = func(*args, **kwargs)

            # 检测是否被拦截
            if isinstance(result, dict) and result.get("error_code") in [403, 401, 10001]:
                self.is_banned = True
                raise Exception(f"请求被雪球拦截，错误码：{result.get('error_code')}，请稍后再试或更换Cookie")

            return result
        except Exception as e:
            if "403" in str(e) or "Forbidden" in str(e):
                self.is_banned = True
                raise Exception("检测到账号已被雪球封禁，请更换Cookie或稍后再试")
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
