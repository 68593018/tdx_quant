# 任务追踪列表

用于追踪基于通达信本地二进制数据的股票量化数据系统开发进度：


## Phase 1: 核心解析与持久化底座 (已完成)
- `[x]` 初始化项目目录与结构 (`tdx_quant/`, `tdx_quant/parser/`, `tdx_quant/storage/`, `tdx_quant/data/`)
- `[x]` 开发 `parser/kline.py` (个股、大盘、板块的日K线二进制极速解析器)
- `[x]` 开发 `parser/gbbq.py` (通达信 GBBQ 股本变迁二进制数据解析器)
- `[x]` 开发 `parser/adjuster.py` (自后向前的高性能前复权算法器)
- `[x]` 开发 `storage/parquet_store.py` (Parquet 列式极速存储与加载层)
- `[x]` 开发并运行 `test_run.py` (多维度数据通路闭环验证脚本)

## Phase 2: 全市场极速同步与数据池构建 (已完成)
- `[x]` 开发 `sync_market.py` (全市场多进程日K线前复权增量同步引擎)
- `[x]` 运行并验证首次全量同步 (9185 只股票/大盘/板块的 Parquet 缓存构建，验证并行耗时统计)
- `[x]` 验证增量更新机制 (再次运行同步脚本，确认是否能在几毫秒内识别并跳过已同步 file)

## Phase 3: 板块数据解析与关联查询 (已完成)
- `[x]` 破解 `block_gn.dat` / `block_fg.dat` / `block_zs.dat` 等板块文件 2813 字节固定记录长度 of 二进制结构
- `[x]` 开发 `parser/block.py`，实现概念、风格、指数三大板块数据的极速全量解析
- `[x]` 将板块同步整合至 `sync_market.py`，支持无缝一键同步全量板块映射关系并输出为 Parquet
- `[x]` 彻底重构 `query.py` 命令行工具，支持板块个股双向关联查询

## Phase 4: 极速 DuckDB 因子选股引擎 (已完成)
- `[x]` 安装 `duckdb` Python 依赖并进行动态安装兼容设计
- `[x]` 编写 `screener.py` 多线程 SQL 策略选股脚本
- `[x]` 通过“自下而上资金集聚宽度”思想设计“概念风口突破共振”量化模型
- `[x]` 在内存中并行对 9,185 只股票千万行数据计算个股均线 (MA20) 与放量均量 (Vol_MA5)
- `[x]` 实现多表极速 INNER JOIN，并完成千万行级复合因子选股计算并降序排列

## Phase 5: 全市场分析与情绪温度引擎 (已完成)
- `[x]` 编写 `strategies.json`，追加全市场涨跌分布、大盘筹码堆积、板块资金宽度和个股连板高度 4 大高能分析 SQL 策略
- `[x]` 编写并部署 `analyzer.py` 市场分析与仪表盘生成核心引擎
- `[x]` 开发 `market_dashboard.html` 赛博黑暗美学本地仪表盘网页模板
- `[x]` 自动运行并生成 `market_analysis_report.md` 精美市场分析报告

## Phase 6: 最近30交易日板块资金流向时序与领涨领跌图 (已完成)
- `[x]` 在 `strategies.json` 中添加 `"industry_flow_30d"` 和 `"concept_flow_30d"` SQL 策略
- `[x]` 修改 `analyzer.py` 载入并执行时序资金流向 SQL 策略，提取最新交易日 TOP 10 和 BOTTOM 10 的 30 日时序数据并进行 JSON 整合
- `[x]` 更新 `analyzer.py` 报告模板，在大屏模板中增加极奢赛博风格的 30 日资金时序折线图
- `[x]` 将项目所有更新提交并同步推送到远程 GitHub 仓库

## Phase 7: B/S Web 量化交易平台后端与 API 服务底座 (已完成)
- `[x]` 在 `server.py` 中使用 FastAPI + Uvicorn 搭建独立量化交易 Web 服务器，具备静默自检依赖功能
- `[x]` 托管 `GET /` 与 `GET /dashboard` 路由，动态注入最新市场大势与情绪 JSON 并渲染免 CORS 看板
- `[x]` 编写动态大势与情绪 RESTful API `GET /api/market/data`（整合 7 大模块，设置 15 秒极速缓存）
- `[x]` 编写多因子选股 API `POST /api/screener/run`（支持多策略动态计算、去噪求交集及股票中文名智能映射）
- `[x]` 编写策略列表接口 `GET /api/strategies` 和策略保存接口 `POST /api/strategies`，实现动态策略持久化
- `[x]` 本地运行服务并进行完整 curl 测试，成功实现 B/S 平台并推送 GitHub

## Phase 8: 统一五大模块量化终端 SPA 看板与离线直开自适应 (已完成)
- `[x]` 创建根目录独立 `web/` 文件夹，开发 Vue 3 赛博玻璃态 Single Page Application (SPA)，无缝集合：总览、多因子选股、大势分析、数据中心、报告归档 5 大核心量化系统
- `[x]` 针对本地双击直开（`file://` 协议）进行智能探测，自动切换为 `DEMO` 离线直开模式，显示琥珀色温馨提示横幅
- `[x]` 完美实现离线渲染：即使离线也可用预注入的静态大盘数据绘制 Overview 区间分布图、 streaks 梯队、筹码带和 30 日资金流向折线图
- `[x]` 实现功能锁定与人机工程关怀：在离线模式下对高级 Python/DuckDB 请求实施半透明遮罩与 Padlock 锁定
- `[x]` 重构并简化 `analyzer.py` 的 HTML 看板生成，使其动态加载 `web/index.html` 模板直接匹配注入，彻底消除 700 行 HTML 字符串冗余
- `[x]` 本地编译与语法校正通过，保持 Uvicorn 服务活跃，并将所有更新推送至 GitHub

## Phase 9: 多因子选股标的范围动态分类筛选 (已完成)
- `[x]` 在 `server.py` 后端注入 `categories` 选股标的动态过滤，并编写映射辅助逻辑
- `[x]` 修复 `get_market_data` 情绪与温度计算中 `__CATEGORY_FILTER__` 缺失的默认替换逻辑
- `[x]` 在前端 `web/index.html` 添加高颜值“🎯 选股标的范围”复选框组件并传入 POST 请求
- `[x]` 运行 `python3 analyzer.py` 编译静态看板大屏文件并验证
- `[x]` 测试 5 类标的（股票、指数、板块、基金、债券）的单独和多选合并筛选功能，确认功能稳定

## Phase 10: 大势分析个股下钻穿透与点击展开交互 (已完成)
- `[x]` SQL 逻辑更新：在 `strategies.json` 里的概念和行业宽度计算中追加 `string_agg` 聚合今日突破个股列表
- `[x]` 后端与编译同步：在 `server.py` 和 `analyzer.py` 中解析突破个股代码、关联本地通达信字典，转换为结构化 `breakout_stocks` 数组
- `[x]` 前端响应式状态：在 `web/index.html` 声明 reactive 变量 `expandedIndustries` 和 `expandedConcepts` 并暴露 toggle 方法
- `[x]` HTML 树状穿透：利用 Vue 3 的 `<template v-for>`，在突破数量上挂载鼠标手势点击事件，下钻显示包含代码和名称的 4 列响应式霓虹网格
- `[x]` 编译验证与缓存同步：重新编译并跑通 `python3 analyzer.py`，确认离线/在线模式点击下钻显示个股均完美生效
