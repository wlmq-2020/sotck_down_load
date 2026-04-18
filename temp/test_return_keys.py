#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pysnowball as xq
import os
from dotenv import load_dotenv

load_dotenv()
xq.set_token(os.getenv('XUEQIU_COOKIE'))

code = 'SZ000422'

methods = [
    ('margin', xq.margin(code)),
    ('blocktrans', xq.blocktrans(code)),
    ('holders', xq.holders(code)),
]

for name, result in methods:
    print(f"\n{name}返回结构：")
    if 'data' in result and isinstance(result['data'], dict):
        print(f"data里的key：{list(result['data'].keys())}")
