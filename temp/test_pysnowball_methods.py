#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试pysnowball各个方法是否可用"""
import os
import pysnowball as xq
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()
xq.set_token(os.getenv('XUEQIU_COOKIE'))

test_code = 'SZ000422'
print(f"测试股票：{test_code}\n")

# 测试各个方法
methods_to_test = [
    ('kline', lambda: xq.kline(test_code, 'day', 1)),
    ('margin', lambda: xq.margin(test_code)),
    ('blocktrans', lambda: xq.blocktrans(test_code)),
    ('holders', lambda: xq.holders(test_code)),
    ('report', lambda: xq.report(test_code, 10)),
    ('org_holding_change', lambda: xq.org_holding_change(test_code)),
    ('f10', lambda: xq.f10(test_code)),
    ('industry', lambda: xq.industry(test_code)),
    ('business_analysis', lambda: xq.business_analysis(test_code)),
    ('bonus', lambda: xq.bonus(test_code)),
    ('shareschg', lambda: xq.shareschg(test_code)),
]

for name, func in methods_to_test:
    try:
        result = func()
        if result and 'data' in result:
            print(f"{name} 调用成功")
            # 打印返回结构示例
            if isinstance(result['data'], dict) and 'items' in result['data']:
                print(f"   返回items数量：{len(result['data']['items'])}")
            elif isinstance(result['data'], list):
                print(f"   返回数据长度：{len(result['data'])}")
        else:
            print(f"{name} 返回为空或无data字段")
    except Exception as e:
        print(f"{name} 调用失败：{str(e)}")
    print("-" * 50)
