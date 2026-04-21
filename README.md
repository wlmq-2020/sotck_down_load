# A股数据获取工具

基于pysnowball开发的A股数据获取工具，支持实时行情、财务数据、资金流向、指数基金、深度资料等多维度数据获取，内置完善的防封策略。

## 功能特性

✅ **5大类数据获取**
1. 实时行情数据：价格、涨跌幅、成交量、买卖盘等实时交易数据
2. 财务基本面数据：财报、利润表、资产负债表、市盈率、ROE等财务指标
3. 资金流向数据：主力资金、北向资金、龙虎榜、大宗交易等
4. 指数与基金数据：大盘指数、行业板块、ETF、公募基金净值等
5. 深度资料：公司公告、研报、融资融券、股东信息等

✅ **完善防封策略**
- 每次请求自动添加1~2秒随机延迟
- 内置频率限制，每分钟最多30次请求
- 异常自动重试，重试间隔指数增长
- 自动识别反爬拦截，封禁提示

✅ **友好使用体验**
- 全中文命令行界面，操作简单
- 支持导出CSV、Excel、JSON格式
- 支持单只/批量股票查询
- 优先支持A股数据，同时兼容美股、港股

## 安装步骤

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 安装工具（可选，用于全局调用）
```bash
pip install -e .
```

### 3. 配置雪球Cookie
1. 复制`.env.example`文件为`.env`
2. 打开浏览器登录雪球官网：https://xueqiu.com/
3. 按F12打开开发者工具 -> 网络面板 -> 刷新页面
4. 找到任意一个xueqiu.com的请求，复制请求头中的Cookie值
5. 将Cookie值粘贴到`.env`文件的`XUEQIU_COOKIE`配置项

## 使用说明

安装完成后，可以通过`stock-download`命令使用所有功能。

### 1. 实时行情查询
```bash
# 查询单只股票行情
stock-download quote 000001.SZ

# 批量查询多只股票行情
stock-download quote 000001.SZ 600000.SH 002594.SZ

# 查询行情并保存到文件
stock-download quote 000001.SZ --output ./data/pingan.csv
```

### 2. 财务数据查询
```bash
# 查询完整财务报表
stock-download finance 000001.SZ

# 只查询利润表
stock-download finance 000001.SZ --report-type income

# 查询资产负债表并保存到Excel
stock-download finance 000001.SZ --report-type balance --output ./data/finance.xlsx
```

### 3. 资金流向查询
```bash
# 查询个股资金流向
stock-download money-flow --symbol 000001.SZ

# 查询北向/南向资金
stock-download money-flow

# 查询龙虎榜数据
stock-download money-flow --lhb

# 查询指定日期龙虎榜
stock-download money-flow --lhb --date 2024-01-01
```

### 4. 指数与基金查询
```bash
# 查询主要大盘指数
stock-download index --major

# 查询行业板块数据
stock-download index --industry

# 查询基金净值
stock-download index --fund 510300.SH

# 查询ETF实时行情
stock-download index --etf 510300.SH
```

### 5. 深度资料查询
```bash
# 查询公司最新公告（默认10条）
stock-download deep 000001.SZ --announcement

# 查询20条公司公告并保存
stock-download deep 000001.SZ --announcement --count 20 --output ./data/announcement.csv

# 查询融资融券数据
stock-download deep 000001.SZ --margin
```

### 6. 定时自动更新
项目提供了开箱即用的Linux定时更新脚本，无需手动执行更新：
```bash
# 1. 进入cron目录
cd cron
# 2. 修改update_daily.sh中的配置，指定你的Python路径和项目部署路径
# 3. 加入定时任务，每天15:30收盘后自动执行
crontab -e
# 添加以下内容：
30 15 * * * /opt/stock_down_load/cron/update_daily.sh
```

脚本自动执行逻辑：
- ✅ 每日更新自选股和所有股票日级数据（行情、资金流、日K线等）
- ✅ 每周日自动执行周级数据更新（财务、公告、机构持仓等）
- ✅ 每月1号自动执行月级数据更新（公司信息、行业分类等）
- ✅ 自动数据质量校验和问题修复
- ✅ 每日生成独立日志文件，方便排查问题

## 目录结构说明
```
├── src/
│   └── stock_download/          # 核心源代码
│       ├── __init__.py          # 版本信息
│       ├── client.py            # 雪球客户端封装（含防封策略）
│       ├── quote.py             # 实时行情模块
│       ├── finance.py           # 财务数据模块
│       ├── money_flow.py        # 资金流向模块
│       ├── index_fund.py        # 指数基金模块
│       ├── deep_data.py         # 深度资料模块
│       └── cli.py               # 命令行界面
├── data/                        # 数据默认保存目录，按功能分类存放
│   ├── quote/                   # 实时行情数据
│   ├── finance/                 # 财务数据
│   ├── money_flow/              # 资金流向数据
│   ├── index_fund/              # 指数基金数据
│   └── deep_data/               # 深度资料数据
├── cron/                        # Linux定时调度脚本目录
├── temp/                        # 临时调试脚本目录（不上传Git）
├── tests/                       # 测试用例
├── .env.example                 # 环境变量模板
├── .env                         # 本地配置（不上传Git）
├── pyproject.toml               # 项目配置
├── requirements.txt             # 依赖列表
├── README.md                    # 使用说明
└── CLAUDE.md                    # 项目开发规范
```

## 注意事项

1. ⚠️ 默认数据范围：仅支持上证（60开头）、深证主板（00开头）股票，自动过滤ST股票、创业板（30开头）、北交所（8/4开头）数据
2. 请合理控制请求频率，避免对雪球服务器造成压力
3. 雪球Cookie有有效期，过期后需要重新获取
4. 本工具仅供学习交流使用，请勿用于商业用途
5. 使用过程中请遵守雪球平台的相关协议和规定

## 问题反馈

如有问题或建议，欢迎提交Issue。
