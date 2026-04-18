#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pysnowball as xq
import os
from dotenv import load_dotenv

load_dotenv()
xq.set_token(os.getenv('XUEQIU_COOKIE'))

code = 'SZ000422'
result = xq.kline(code, 'day', 1)
print("kline返回结构：")
print(result.keys())
if 'data' in result:
    print("data字段类型：", type(result['data']))
    if isinstance(result['data'], dict):
        print("data包含的key：", result['data'].keys())
