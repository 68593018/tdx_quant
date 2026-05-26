# 🚀 多因子选股标的范围动态分类筛选设计方案

当前量化分析系统的数据池中混合了**股票、指数、板块、基金、债券**等多种类型的 Parquet 日K线数据。由于此前所有的选股策略均未对标的类别进行区分，导致在执行多因子选股时，结果中会混杂大盘指数、板块指数、公募基金或可转债，无法满足用户针对特定标的范围进行精确选股的需求。

本方案旨在：
1. 在 **FastAPI 后端 (`server.py`)** 引入动态标的分选过滤逻辑，利用 Parquet 日K线文件名前缀特征，支持用户自选的类别范围拼接 SQL 过滤表达式，并替换策略 SQL 中的 `__CATEGORY_FILTER__` 占位符。
2. 在 **Vue 交互前端 (`web/index.html`)** 的策略选股面板中，新增一个高颜值的玻璃态“🎯 选股标的范围”复选框组件，允许用户勾选一个或多个类别（股票、指数、板块、基金、债券），并在运行选股时将其提交至后端。
3. 确保宏观分析接口（如大势温度、涨跌分布等）在后端调用 SQL 时默认替换为标准 A 股过滤，以保持大盘情绪分析的纯净度和一致性。

---

## User Review Required

> [!IMPORTANT]
> - **默认值设定**：默认进入页面时只勾选“股票（A股）”类别，以保证绝大多数用户的常规选股体验，避免因为全选导致非股票标的混入。
> - **空勾选容错**：如果用户取消勾选了所有标的类别，系统将自动回退并默认以“股票”为范围进行计算，避免由于过滤条件为空导致 SQL 语法错误或返回空结果。

---

## Open Questions

> [!NOTE]
> - **关于北京证券交易所 (BJ) 股票的匹配**：在数据同步模块中，北交所股票保存为 `bjXXXXXX.parquet`（如 `bj830000.parquet`），在 DuckDB 中其路径匹配表达式为 `filename LIKE '%bj%'`。我们将沿用此映射规则，确保北证 A 股选股不遗漏。

---

## Proposed Changes

### 1. 后端服务逻辑 (Backend Service)

#### [MODIFY] [server.py](file:///home/liliiflora/work/wsl-agy-projects/tdx_quant/server.py)
* **Pydantic 模型更新**：
  在 `ScreenerRequest` 中新增 `categories` 列表参数：
  ```python
  class ScreenerRequest(BaseModel):
      strategies: list[str]
      categories: list[str] = ["stock"]
  ```
* **标的分类过滤辅助函数**：
  新增 `generate_category_filter(categories: list[str]) -> str`，根据传入的类别标识符（`stock`, `index`, `sector`, `fund`, `bond`）映射为对应的 `filename LIKE` 过滤 SQL 子句：
  - `stock`: `(filename LIKE '%sh60%' OR filename LIKE '%sh68%' OR filename LIKE '%sz00%' OR filename LIKE '%sz30%' OR filename LIKE '%/bj%')`
  - `index`: `(filename LIKE '%sh000%' OR filename LIKE '%sz399%')`
  - `sector`: `(filename LIKE '%sh88%' OR filename LIKE '%sz88%')`
  - `fund`: `(filename LIKE '%sh50%' OR filename LIKE '%sh51%' OR filename LIKE '%sh52%' OR filename LIKE '%sh58%' OR filename LIKE '%sz15%' OR filename LIKE '%sz16%' OR filename LIKE '%sz18%')`
  - `bond`: `(filename LIKE '%sh11%' OR filename LIKE '%sh13%' OR filename LIKE '%sz12%')`
* **`run_screener` 接口逻辑注入**：
  在 `run_screener` 中提取 `req.categories`，生成对应的 `category_filter`。在执行各策略 SQL 前，执行 `.replace("__CATEGORY_FILTER__", category_filter)`。
* **`get_market_data` 接口逻辑注入**：
  在 7 大分析策略（如大势温度、涨跌分布、连板高度等）的 SQL 计算前，统一将 `__CATEGORY_FILTER__` 替换为默认的 `stock` 过滤子句。这样即使这些分析策略的 SQL 中带有占位符，也能稳定执行且不含非股票数据。

---

### 2. 前端交互界面 (Frontend Interface)

#### [MODIFY] [index.html](file:///home/liliiflora/work/wsl-agy-projects/tdx_quant/web/index.html)
* **新增 UI 标的筛选组件**：
  在 Tab 2 左侧的策略目录上方，插入一个玻璃态的 “🎯 选股标的范围” 复选面板。外观采用赛博朋克深色调与霓虹青色勾选框风格，包含 5 大类别复选按钮：
  - [x] 股票 (A股)
  - [ ] 指数
  - [ ] 板块
  - [ ] 基金
  - [ ] 债券 (可转债等)
* **Vue 状态与逻辑集成**：
  - 在 `setup()` 中声明 `screenerCategories = ref(["stock"])` 响应式数据。
  - 在 `runScreener()` 方法中，将 `categories: screenerCategories.value` 作为参数通过 POST 请求体发送至 `/api/screener/run`
  - 在 `setup()` 的 return 对象中暴露 `screenerCategories`。

---

### 3. 数据编译与分析引擎 (Quant Dashboard Compiler)

#### [MODIFY] [analyzer.py](file:///home/liliiflora/work/wsl-agy-projects/tdx_quant/analyzer.py)
* 在主程序执行结束后运行 `generate_html_dashboard` 时，它会读取修改后的 `web/index.html`，这会确保静态编译的本地大屏文件 `market_dashboard.html` 保持最新的页面结构与前台离线展示数据。

---

## Verification Plan

### 自动化验证与功能联调
1. **测试后端启动与重载**：
   确认 `server.py` 监听 `http://localhost:8000` 并通过 Uvicorn 自动重载生效。
2. **股票/指数/板块多选隔离测试**：
   - 打开浏览器，进入 **“策略选股”** 面板。
   - **测试1：单选股票**。勾选任一策略（如“共振突破选股”），标的范围仅保留“股票”，执行选股。确认计算速度飞快，且结果中全部为 A 股个股（如 `SH60...` / `SZ00...` 等），不掺杂任何板块指数或转债。
   - **测试2：多选基金与债券**。取消勾选股票，勾选“基金”和“债券”，执行选股。验证计算结果是否均属于公募基金和转债范围，无任何股票或指数混入。
   - **测试3：全类别多选求合力**。全选 5 个标的范围，验证 DuckDB 是否能正确执行并完美返回全种类标的的多因子混合排名。
3. **大盘分析接口安全验证**：
   - 切换到 **“市场总览”** 或 **“大势分析”** 面板。
   - 验证市场温度折线图、行业占比图以及大势雷达图是否正常加载、完全无后端报错。这说明 `get_market_data` 完美兼顾并替换了分析 SQL 中的占位符。
4. **编译同步更新**：
   - 运行终端命令 `python3 analyzer.py`，确认无任何 Python 编译报错，且本地 `market_dashboard.html` 生成成功。
