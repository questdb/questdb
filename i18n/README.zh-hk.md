<div align="center">
  <a href="https://questdb.io/" target="blank">
    <img alt="QuestDB 標誌" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/>
  </a>
</div>
<p>&nbsp;</p>

<p align="center">
  <a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="QuestDB 社群 Slack 頻道"/>
  </a>
  <a href="#contribute">
    <img src="https://img.shields.io/github/contributors/questdb/questdb" alt="QuestDB 開源貢獻者"/>
  </a>
  <a href="https://search.maven.org/search?q=g:org.questdb">
    <img src="https://img.shields.io/maven-central/v/org.questdb/questdb" alt="QuestDB on Apache Maven"/>
  </a>
</p>

<p align="center">
  <a href="https://github.com/questdb/questdb">English</a> |
  <a href="README.zh-cn.md">简体中文</a> |
  繁體中文 |
  <a href="README.it-it.md">Italiano</a> |
  <a href="README.ua-ua.md">Українська</a> |
  <a href="README.es-es.md">Español</a> |
  <a href="README.pt.md">Português</a> |
  <a href="README.ja-ja.md">日本語</a> |
  <a href="README.tr-tr.md">Türkçe</a> |
  <a href="README.hn-in.md">हिंदी</a> |
  <a href="README.vi-vn.md">Tiếng Việt</a>
</p>

# QuestDB

QuestDB 是目前成長最快的開源時序數據庫，提供 **極速高吞吐量的數據寫入** 與 **動態、低延遲的 SQL 查詢**。整個高性能程式碼庫是從零開始使用 Java、C++ 與 Rust 架構開發，無任何外部依賴，且 100% 無垃圾回收。

我們通過以下技術實現高性能：
- 以欄為導向的存儲模型
- 向量化平行執行
- SIMD 指令
- 低延遲技術

此外，QuestDB 在硬體使用上非常高效，啟動迅速，運行簡單。

QuestDB 使用原生時序 SQL 擴展來實現 ANSI SQL。這些 SQL 擴展使得對數據的分析、過濾及降採樣變得更容易，同時也能夠利用關聯和時序連接來關聯來自多個來源的數據。

> 準備好了嗎？請跳轉至 [開始使用](#開始使用) 部分。

<p>&nbsp;</p>

<div align="center">
  <a href="https://demo.questdb.io/">
    <img alt="QuestDB Web Console 顯示 SQL 語句與查詢結果" src="https://raw.githubusercontent.com/questdb/questdb/master/.github/console.png" width="600" />
  </a>
  <p><em>QuestDB Web Console – 點擊啟動 Demo</em></p>
</div>

<p>&nbsp;</p>

## QuestDB 的優勢

QuestDB 特別適用於：
- 金融市場數據
- 高基數的 IoT 感測器數據
- 實時儀表板

主要特點包括：
- 內建強大且經過 SIMD 優化的時序 SQL 擴展
- 使用 InfluxDB Line Protocol 實現高速數據寫入
- 即使在硬體有限的情況下也能表現出強大且高效的性能
- 支持原生或 [Apache Parquet](https://questdb.io/glossary/apache-parquet/) 的欄式存儲格式，並根據時間進行分區和排序
- 直覺且響應迅速的 Web Console 用於查詢和數據管理，內建錯誤處理
- 即使在[高基數](https://questdb.io/glossary/high-cardinality/)數據下也有卓越表現 —— 請參閱 [Benchmark](#questdb-performance-vs-other-oss-databases)

### 為什麼選擇時序數據庫？

除了性能與效率之外，使用專門的時序數據庫可以免去以下煩惱：
- 數據無序
- 重複數據
- “正好一條”語義問題
- 流式數據（低延遲）
- 大量併發請求
- 數據突發（bursty）
- 數據寫入時動態修改結構（新增欄位）

## 試用 QuestDB

我們提供一個 [線上 Demo](https://demo.questdb.io/) ，內置最新版本的 QuestDB 及樣本數據集：
- **Trips：** 紐約市 10 年的計程車行程數據，包含 16 億行
- **Trades：** 每月超過 3000 萬行的實時加密貨幣市場數據
- **Pos：** 追蹤 25 萬艘獨特船舶地理位置的數據

您可以使用示例查詢，或自行撰寫查詢！

_注意：公共 Demo 查詢超過 16 億行數據，並使用一個 `r6a.12xlarge` 實例（48 vCPU，348GB RAM）。_

| 查詢                                                                          | 運行時間                                                                                                                                                                                            |
|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT sum(double) FROM trips`                                               | [0.15 secs](https://demo.questdb.io/?query=SELECT%20sum(trip_distance)%20FROM%20trips;&executeQuery=true)                                                                                           |
| `SELECT sum(double), avg(double) FROM trips`                                  | [0.5 secs](https://demo.questdb.io/?query=SELECT%20sum(fare_amount),%20avg(fare_amount)%20FROM%20trips;&executeQuery=true)                                                                              |
| `SELECT avg(double) FROM trips WHERE time in '2019'`                          | [0.02 secs](https://demo.questdb.io/?query=SELECT%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019%27;&executeQuery=true)                                             |
| `SELECT time, avg(double) FROM trips WHERE time in '2019-01-01' SAMPLE BY 1h` | [0.01 secs](https://demo.questdb.io/?query=SELECT%20pickup_datetime,%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019-01-01%27%20SAMPLE%20BY%201h;&executeQuery=true) |
| `SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol`                | [0.00025 secs](https://demo.questdb.io/?query=SELECT%20*%20FROM%20trades%20LATEST%20ON%20timestamp%20PARTITION%20BY%20symbol;&executeQuery=true)                                                |

我們的線上 Demo 運行於 `c5.metal` 實例上，僅使用 96 個線程中的 24 個線程。

## QuestDB Performance vs. Other OSS Databases

QuestDB 在 benchmark 測試中展現出超越其他開源數據庫的卓越性能。

想了解更多內部細節和性能資訊，請參考以下部落格文章：
- [QuestDB vs InfluxDB](https://questdb.io/blog/2024/02/26/questdb-versus-influxdb/)
- [QuestDB vs TimescaleDB](https://questdb.io/blog/timescaledb-vs-questdb-comparison/)
- [QuestDB vs MongoDB](https://questdb.io/blog/mongodb-time-series-benchmark-review/)

當然，我們也鼓勵您自己進行 benchmark 測試。

<div align="center">
  <img alt="比較 QuestDB、InfluxDB 與 TimescaleDB 數據寫入速率的圖表" src="https://user-images.githubusercontent.com/91843271/197382161-e5f3f5b0-18bf-439a-94e4-83ab4bf91d7c.png" width="600"/>
</div>

## 開始使用

### 安裝 QuestDB

要快速運行 QuestDB，您可以使用 [Docker](https://www.docker.com/)：

```bash
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```

macOS 用戶可以使用 Homebrew：

```bash
brew install questdb
brew services start questdb

questdb start   # 啟動 QuestDB
questdb stop    # 停止 QuestDB
```

另外，若要完整體驗上手流程，請參閱我們的 [快速入門指南](https://questdb.io/docs/quick-start/)。

### QuestDB Cloud

QuestDB Cloud 是 QuestDB 的全托管版本，提供額外功能，如基於角色的存取控制 (RBAC)、原生雲多主寫入、冷存儲整合、TLS 加密、以及針對批量數據查詢的 REST API 等。  
[立即免費試用，享受 $200 信用額度](https://cloud.questdb.com)。

### 連接到 QuestDB

您可以通過以下接口與 QuestDB 及其數據進行交互：

- [Web Console](https://questdb.io/docs/web-console/) – 互動式 SQL 編輯器與 CSV 導入，預設在 `9000` 端口運行
- [InfluxDB Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/) – 用於流式數據寫入，預設在 `9000` 端口
- [PostgreSQL Wire Protocol](https://questdb.io/docs/reference/api/postgres/) – 用於程式化查詢，預設在 `8812` 端口
- [REST API](https://questdb.io/docs/reference/api/rest/) – 用於 CSV 導入與 cURL 請求，預設在 `9000` 端口

### 官方數據寫入客戶端

QuestDB 為多種程式語言提供官方客戶端，以通過 InfluxDB Line Protocol 寫入數據：

- [Python](https://questdb.io/docs/clients/ingest-python/)
- [.NET](https://questdb.io/docs/clients/ingest-dotnet/)
- [C/C++](https://questdb.io/docs/clients/ingest-c-and-cpp/)
- [Go](https://questdb.io/docs/clients/ingest-go/)
- [Java](https://questdb.io/docs/clients/java_ilp/)
- [NodeJS](https://questdb.io/docs/clients/ingest-node/)
- [Rust](https://questdb.io/docs/clients/ingest-rust/)

### 與 QuestDB 互動

您可以透過以下接口與 QuestDB 及數據互動：

- [Web Console](https://questdb.io/docs/web-console/) – 互動式 SQL 編輯器和 CSV 導入（端口 `9000`）
- [InfluxDB Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/) – 流式數據寫入（端口 `9000`）
- [PostgreSQL Wire Protocol](https://questdb.io/docs/reference/api/postgres/) – 程式化查詢（端口 `8812`）
- [REST API](https://questdb.io/docs/reference/api/rest/) – CSV 導入及 cURL 請求（端口 `9000`）

### 第三方工具

QuestDB 與許多流行工具整合，包括：
- [Apache Kafka](https://questdb.io/docs/third-party-tools/kafka/)
- [Grafana](https://questdb.io/docs/third-party-tools/grafana/)
- [Superset](https://questdb.io/docs/third-party-tools/superset/)
- [Telegraf](https://questdb.io/docs/third-party-tools/telegraf/)
- [Apache Flink](https://questdb.io/docs/third-party-tools/flink/)
- [qStudio](https://questdb.io/docs/third-party-tools/qstudio/)
- [MindsDB](https://questdb.io/docs/third-party-tools/mindsdb/)

### 端到端範例

如果您想體驗從流式數據寫入到使用 Grafana 進行視覺化的完整流程，請參考我們的 [Quickstart Repository](https://github.com/questdb/questdb-quickstart)。

### 為生產環境配置 QuestDB

請參閱我們的 [Capacity Planning 指南](https://questdb.io/docs/deployment/capacity-planning/)，以優化 QuestDB 以適應生產工作負載。

### QuestDB Enterprise

對於大規模安全運作或在大型組織內使用，QuestDB Enterprise 提供以下附加功能：

- 多主數據寫入
- 讀取副本
- 冷存儲整合
- 基於角色的存取控制 (RBAC)
- TLS 加密
- 透過 Object Storage 對 Parquet 檔案進行原生查詢
- 支援 SLA、增強監控及更多

請參閱 [Enterprise 頁面](https://questdb.io/enterprise/) 獲取更多詳情與聯繫方式。

## 附加資源

### 📚 閱讀文檔

- [QuestDB 文檔:](https://questdb.io/docs/introduction/) 了解如何運行和配置 QuestDB。
- [Tutorials:](https://questdb.io/tutorial/) 一步一步學習 QuestDB 的功能。
- [產品路線圖:](https://github.com/orgs/questdb/projects/1/views/5) 查看我們未來版本的計劃。

### ❓ 獲取支持

- [Community Discourse 論壇:](https://community.questdb.io/) 加入技術討論，提問並結識其他用戶！
- [公共 Slack:](https://slack.questdb.io/) 與 QuestDB 團隊及社群成員進行即時交流。
- [GitHub Issues:](https://github.com/questdb/questdb/issues) 報告 QuestDB 的 Bug 或問題。
- [Stack Overflow:](https://stackoverflow.com/questions/tagged/questdb) 查找常見問題的解決方案。

### 🚢 部署 QuestDB

- [AWS AMI](https://questdb.io/docs/guides/aws-official-ami)
- [Google Cloud Platform](https://questdb.io/docs/guides/google-cloud-platform)
- [官方 Docker 映像](https://questdb.io/docs/get-started/docker)
- [DigitalOcean Droplets](https://questdb.io/docs/guides/digitalocean)
- [Kubernetes Helm Charts](https://questdb.io/docs/guides/kubernetes)

## 貢獻

我們非常歡迎任何形式的貢獻，無論是源碼、文檔、錯誤報告、功能請求或反饋。

### 如何貢獻

- 查看 GitHub 上標有 "[Good first issue](https://github.com/questdb/questdb/issues?q=is%3Aissue+is%3Aopen+label%3A%22Good+first+issue%22)" 的問題。
- 閱讀 [貢獻指南](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md)。
- 有關如何構建 QuestDB 的詳細資訊，請參閱 [構建說明](https://github.com/questdb/questdb/blob/master/core/README.md)。
- [建立一個 QuestDB 的 fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) 並提交 pull request，說明您建議的修改。
- 如果遇到困難，請加入我們的 [公共 Slack](https://slack.questdb.io/) 獲取協助。

✨ 為表達我們的感謝，我們也會向貢獻者送出 **QuestDB 周邊產品**。  
[點此領取您的周邊產品](https://questdb.io/community).

衷心感謝以下為 QuestDB 作出貢獻的優秀人士：  
([emoji key](https://allcontributors.org/docs/en/emoji-key))。

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/clickingbuttons"><img src="https://avatars1.githubusercontent.com/u/43246297?v=4" width="100px;" alt=""/><br /><sub><b>clickingbuttons</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=clickingbuttons" title="Code">💻</a> <a href="#ideas-clickingbuttons" title="Ideas, Planning, & Feedback">🤔</a> <a href="#userTesting-clickingbuttons" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ideoma"><img src="https://avatars0.githubusercontent.com/u/2159629?v=4" width="100px;" alt=""/><br /><sub><b>ideoma</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=ideoma" title="Code">💻</a> <a href="#userTesting-ideoma" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/commits?author=ideoma" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tonytamwk"><img src="https://avatars2.githubusercontent.com/u/20872271?v=4" width="100px;" alt=""/><br /><sub><b>tonytamwk</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=tonytamwk" title="Code">💻</a> <a href="#userTesting-tonytamwk" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://sirinath.com/"><img src="https://avatars2.githubusercontent.com/u/637415?v=4" width="100px;" alt=""/><br /><sub><b>sirinath</b></sub></a><br /><a href="#ideas-sirinath" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/suhorukov"><img src="https://avatars1.githubusercontent.com/u/10332206?v=4" width="100px;" alt=""/><br /><sub><b>igor-suhorukov</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=igor-suhorukov" title="Code">💻</a> <a href="#ideas-igor-suhorukov" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mick2004"><img src="https://avatars1.githubusercontent.com/u/2042132?v=4" width="100px;" alt=""/><br /><sub><b>mick2004</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=mick2004" title="Code">💻</a> <a href="#platform-mick2004" title="Packaging/porting to new platform">📦</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://rawkode.com"><img src="https://avatars3.githubusercontent.com/u/145816?v=4" width="100px;" alt=""/><br /><sub><b>rawkode</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=rawkode" title="Code">💻</a> <a href="#infra-rawkode" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://solidnerd.dev"><img src="https://avatars0.githubusercontent.com/u/886383?v=4" width="100px;" alt=""/><br /><sub><b>solidnerd</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=solidnerd" title="Code">💻</a> <a href="#infra-solidnerd" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://solanav.github.io"><img src="https://avatars1.githubusercontent.com/u/32469597?v=4" width="100px;" alt=""/><br /><sub><b>solanav</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=solanav" title="Code">💻</a> <a href="https://github.com/questdb/questdb/commits?author=solanav" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://shantanoo-desai.github.io"><img src="https://avatars1.githubusercontent.com/u/12070966?v=4" width="100px;" alt=""/><br /><sub><b>shantanoo-desai</b></sub></a><br /><a href="#blog-shantanoo-desai" title="Blogposts">📝</a> <a href="#example-shantanoo-desai" title="Examples">💡</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://alexprut.com"><img src="https://avatars2.githubusercontent.com/u/1648497?v=4" width="100px;" alt=""/><br /><sub><b>alexprut</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=alexprut" title="Code">💻</a> <a href="#maintenance-alexprut" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/lbowman"><img src="https://avatars1.githubusercontent.com/u/1477427?v=4" width="100px;" alt=""/><br /><sub><b>lbowman</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=lbowman" title="Code">💻</a> <a href="https://github.com/questdb/questdb/commits?author=lbowman" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://tutswiki.com/"><img src="https://avatars1.githubusercontent.com/u/424822?v=4" width="100px;" alt=""/><br /><sub><b>chankeypathak</b></sub></a><br /><a href="#blog-chankeypathak" title="Blogposts">📝</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/upsidedownsmile"><img src="https://avatars0.githubusercontent.com/u/26444088?v=4" width="100px;" alt=""/><br /><sub><b>upsidedownsmile</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=upsidedownsmile" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Nagriar"><img src="https://avatars0.githubusercontent.com/u/2361099?v=4" width="100px;" alt=""/><br /><sub><b>Nagriar</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=Nagriar" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/piotrrzysko"><img src="https://avatars.githubusercontent.com/u/6481553?v=4" width="100px;" alt=""/><br /><sub><b>piotrrzysko</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=piotrrzysko" title="Code">💻</a> <a href="https://github.com/questdb/questdb/commits?author=piotrrzysko" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mpsq/dotfiles"><img src="https://avatars.githubusercontent.com/u/5734722?v=4" width="100px;" alt=""/><br /><sub><b>mpsq</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=mpsq" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/siddheshlatkar"><img src="https://avatars.githubusercontent.com/u/39632173?v=4" width="100px;" alt=""/><br /><sub><b>siddheshlatkar</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=siddheshlatkar" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://yitaekhwang.com"><img src="https://avatars.githubusercontent.com/u/6628444?v=4" width="100px;" alt=""/><br /><sub><b>Yitaek</b></sub></a><br /><a href="#tutorial-Yitaek" title="Tutorials">✅</a> <a href="#example-Yitaek" title="Examples">💡</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.gaboros.hu"><img src="https://avatars.githubusercontent.com/u/19173947?v=4" width="100px;" alt=""/><br /><sub><b>gabor-boros</b></sub></a><br /><a href="#tutorial-gabor-boros" title="Tutorials">✅</a> <a href="#example-gabor-boros" title="Examples">💡</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kovid-r"><img src="https://avatars.githubusercontent.com/u/62409489?v=4" width="100px;" alt=""/><br /><sub><b>kovid-r</b></sub></a><br /><a href="#tutorial-kovid-r" title="Tutorials">✅</a> <a href="#example-kovid-r" title="Examples">💡</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://borowski-software.de/"><img src="https://avatars.githubusercontent.com/u/8701341?v=4" width="100px;" alt=""/><br /><sub><b>TimBo93</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3ATimBo93" title="Bug reports">🐛</a> <a href="#userTesting-TimBo93" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://zikani.me"><img src="https://avatars.githubusercontent.com/u/1501387?v=4" width="100px;" alt=""/><br /><sub><b>zikani03</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=zikani03" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jaugsburger"><img src="https://avatars.githubusercontent.com/u/10787042?v=4" width="100px;" alt=""/><br /><sub><b>jaugsburger</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=jaugsburger" title="Code">💻</a> <a href="#maintenance-jaugsburger" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://www.questdb.io"><img src="https://avatars.githubusercontent.com/u/52114895?v=4" width="100px;" alt=""/><br /><sub><b>TheTanc</b></sub></a><br /><a href="#projectManagement-TheTanc" title="Project Management">📆</a> <a href="#content-TheTanc" title="Content">🖋</a> <a href="#ideas-TheTanc" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://davidgs.com"><img src="https://avatars.githubusercontent.com/u/2071898?v=4" width="100px;" alt=""/><br /><sub><b>davidgs</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Adavidgs" title="Bug reports">🐛</a> <a href="#content-davidgs" title="Content">🖋</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://redalemeden.com"><img src="https://avatars.githubusercontent.com/u/519433?v=4" width="100px;" alt=""/><br /><sub><b>kaishin</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=kaishin" title="Code">💻</a> <a href="#example-kaishin" title="Examples">💡</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://questdb.io"><img src="https://avatars.githubusercontent.com/u/7276403?v=4" width="100px;" alt=""/><br /><sub><b>bluestreak01</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=bluestreak01" title="Code">💻</a> <a href="#maintenance-bluestreak01" title="Maintenance">🚧</a> <a href="https://github.com/questdb/questdb/commits?author=bluestreak01" title="Tests">⚠️</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://patrick.spacesurfer.com/"><img src="https://avatars.githubusercontent.com/u/29952889?v=4" width="100px;" alt=""/><br /><sub><b>patrickSpaceSurfer</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=patrickSpaceSurfer" title="Code">💻</a> <a href="#maintenance-patrickSpaceSurfer" title="Maintenance">🚧</a> <a href="https://github.com/questdb/questdb/commits?author=patrickSpaceSurfer" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://chenrui.dev"><img src="https://avatars.githubusercontent.com/u/1580956?v=4" width="100px;" alt=""/><br /><sub><b>chenrui333</b></sub></a><br /><a href="#infra-chenrui333" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://bsmth.de"><img src="https://avatars.githubusercontent.com/u/43580235?v=4" width="100px;" alt=""/><br /><sub><b>bsmth</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=bsmth" title="Documentation">📖</a> <a href="#content-bsmth" title="Content">🖋</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Ugbot"><img src="https://avatars.githubusercontent.com/u/2143631?v=4" width="100px;" alt=""/><br /><sub><b>Ugbot</b></sub></a><br /><a href="#question-Ugbot" title="Answering Questions">💬</a> <a href="#userTesting-Ugbot" title="User Testing">📓</a> <a href="#talk-Ugbot" title="Talks">📢</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/lepolac"><img src="https://avatars.githubusercontent.com/u/6312424?v=4" width="100px;" alt=""/><br /><sub><b>lepolac</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=lepolac" title="Code">💻</a> <a href="#tool-lepolac" title="Tools">🔧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tiagostutz"><img src="https://avatars.githubusercontent.com/u/3986989?v=4" width="100px;" alt=""/><br /><sub><b>tiagostutz</b></sub></a><br /><a href="#userTesting-tiagostutz" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Atiagostutz" title="Bug reports">🐛</a> <a href="#projectManagement-tiagostutz" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Lyncee59"><img src="https://avatars.githubusercontent.com/u/13176504?v=4" width="100px;" alt=""/><br /><sub><b>Lyncee59</b></sub></a><br /><a href="#ideas-Lyncee59" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/questdb/questdb/commits?author=Lyncee59" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/rrjanbiah"><img src="https://avatars.githubusercontent.com/u/4907427?v=4" width="100px;" alt=""/><br /><sub><b>rrjanbiah</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Arrjanbiah" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/sarunas-stasaitis"><img src="https://avatars.githubusercontent.com/u/57004257?v=4" width="100px;" alt=""/><br /><sub><b>sarunas-stasaitis</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Asarunas-stasaitis" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/RiccardoGiro"><img src="https://avatars.githubusercontent.com/u/60734967?v=4" width="100px;" alt=""/><br /><sub><b>RiccardoGiro</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3ARiccardoGiro" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/duggar"><img src="https://avatars.githubusercontent.com/u/37486846?v=4" width="100px;" alt=""/><br /><sub><b>duggar</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Aduggar" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/postol"><img src="https://avatars.githubusercontent.com/u/7983951?v=4" width="100px;" alt=""/><br /><sub><b>postol</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Apostol" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/petrjahoda"><img src="https://avatars.githubusercontent.com/u/45359845?v=4" width="100px;" alt=""/><br /><sub><b>petrjahoda</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Apetrjahoda" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.turecki.net"><img src="https://avatars.githubusercontent.com/u/1933165?v=4" width="100px;" alt=""/><br /><sub><b>t00</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3At00" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/snenkov"><img src="https://avatars.githubusercontent.com/u/13110986?v=4" width="100px;" alt=""/><br /><sub><b>snenkov</b></sub></a><br /><a href="#userTesting-snenkov" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Asnenkov" title="Bug reports">🐛</a> <a href="#ideas-snenkov" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/marregui"><img src="https://avatars.githubusercontent.com/u/255796?v=4" width="100px;" alt=""/><br /><sub><b>marregui</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=marregui" title="Code">💻</a> <a href="#ideas-marregui" title="Ideas, Planning, & Feedback">🤔</a> <a href="#design-marregui" title="Design">🎨</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/bratseth"><img src="https://avatars.githubusercontent.com/u/16574012?v=4" width="100px;" alt=""/><br /><sub><b>bratseth</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=bratseth" title="Code">💻</a> <a href="#ideas-bratseth" title="Ideas, Planning, & Feedback">🤔</a> <a href="#userTesting-bratseth" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://medium.com/@wellytambunan/"><img src="https://avatars.githubusercontent.com/u/242694?v=4" width="100px;" alt=""/><br /><sub><b>welly87</b></sub></a><br /><a href="#ideas-welly87" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://johnleung.com"><img src="https://avatars.githubusercontent.com/u/20699?v=4" width="100px;" alt=""/><br /><sub><b>fuzzthink</b></sub></a><br /><a href="#ideas-fuzzthink" title="Ideas, Planning, & Feedback">🤔</a> <a href="#userTesting-fuzzthink" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/nexthack"><img src="https://avatars.githubusercontent.com/u/6803956?v=4" width="100px;" alt=""/><br /><sub><b>nexthack</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=nexthack" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/g-metan"><img src="https://avatars.githubusercontent.com/u/88013490?v=4" width="100px;" alt=""/><br /><sub><b>g-metan</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Ag-metan" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tim2skew"><img src="https://avatars.githubusercontent.com/u/54268285?v=4" width="100px;" alt=""/><br /><sub><b>tim2skew</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Atim2skew" title="Bug reports">🐛</a> <a href="#userTesting-tim2skew" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ospqsp"><img src="https://avatars.githubusercontent.com/u/84992434?v=4" width="100px;" alt=""/><br /><sub><b>ospqsp</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Aospqsp" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/SuperFluffy"><img src="https://avatars.githubusercontent.com/u/701177?v=4" width="100px;" alt=""/><br /><sub><b>SuperFluffy</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3ASuperFluffy" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/nu11ptr"><img src="https://avatars.githubusercontent.com/u/3615587?v=4" width="100px;" alt=""/><br /><sub><b>nu11ptr</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Anu11ptr" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/comunidadio"><img src="https://avatars.githubusercontent.com/u/10286013?v=4" width="100px;" alt=""/><br /><sub><b>comunidadio</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Acomunidadio" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mugendi"><img src="https://avatars.githubusercontent.com/u/5348246?v=4" width="100px;" alt=""/><br /><sub><b>mugendi</b></sub></a><br /><a href="#ideas-mugendi" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Amugendi" title="Bug reports">🐛</a> <a href="https://github.com/questdb/questdb/commits?author=mugendi" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/paulwoods222"><img src="https://avatars.githubusercontent.com/u/86227717?v=4" width="100px;" alt=""/><br /><sub><b>paulwoods222</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Apaulwoods222" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mingodad"><img src="https://avatars.githubusercontent.com/u/462618?v=4" width="100px;" alt=""/><br /><sub><b>mingodad</b></sub></a><br /><a href="#ideas-mingodad" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Amingodad" title="Bug reports">🐛</a> <a href="https://github.com/questdb/questdb/commits?author=mingodad" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/houarizegai"><img src="https://avatars.githubusercontent.com/houarizegai?v=4" width="100px;" alt=""/><br /><sub><b>houarizegai</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=houarizegai" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://scrapfly.io"><img src="https://avatars.githubusercontent.com/u/1763341?v=4" width="100px;" alt=""/><br /><sub><b>jjsaunier</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Ajjsaunier" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/zanek"><img src="https://avatars.githubusercontent.com/u/333102?v=4" width="100px;" alt=""/><br /><sub><b>zanek</b></sub></a><br /><a href="#ideas-zanek" title="Ideas, Planning, & Feedback">🤔</a> <a href="#projectManagement-zanek" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Geekaylee"><img src="https://avatars.githubusercontent.com/u/12583377?v=4" width="100px;" alt=""/><br /><sub><b>Geekaylee</b></sub></a><br /><a href="#userTesting-Geekaylee" title="User Testing">📓</a> <a href="#ideas-Geekaylee" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/lg31415"><img src="https://avatars.githubusercontent.com/u/3609384?v=4" width="100px;" alt=""/><br /><sub><b>lg31415</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Alg31415" title="Bug reports">🐛</a> <a href="#projectManagement-lg31415" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://nulldev.xyz/"><img src="https://avatars.githubusercontent.com/u/9571936?v=4" width="100px;" alt=""/><br /><sub><b>null-dev</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Anull-dev" title="Bug reports">🐛</a> <a href="#projectManagement-null-dev" title="Project Management">📆</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://ultd.io"><img src="https://avatars.githubusercontent.com/u/12675427?v=4" width="100px;" alt=""/><br /><sub><b>ultd</b></sub></a><br /><a href="#ideas-ultd" title="Ideas, Planning, & Feedback">🤔</a> <a href="#projectManagement-ultd" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ericsun2"><img src="https://avatars.githubusercontent.com/u/8866410?v=4" width="100px;" alt=""/><br /><sub><b>ericsun2</b></sub></a><br /><a href="#ideas-ericsun2" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Aericsun2" title="Bug reports">🐛</a> <a href="#projectManagement-ericsun2" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/giovanni-k-bonetti-2809345/"><img src="https://avatars.githubusercontent.com/u/3451581?v=4" width="100px;" alt=""/><br /><sub><b>giovannibonetti</b></sub></a><br /><a href="#userTesting-giovannibonetti" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Agiovannibonetti" title="Bug reports">🐛</a> <a href="#projectManagement-giovannibonetti" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://wavded.com"><img src="https://avatars.githubusercontent.com/u/26638?v=4" width="100px;" alt=""/><br /><sub><b>wavded</b></sub></a><br /><a href="#userTesting-wavded" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Awavded" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://medium.com/@apechkurov"><img src="https://avatars.githubusercontent.com/u/37772591?v=4" width="100px;" alt=""/><br /><sub><b>puzpuzpuz</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=puzpuzpuz" title="Documentation">📖</a> <a href="https://github.com/questdb/questdb/commits?author=puzpuzpuz" title="Code">💻</a> <a href="#userTesting-puzpuzpuz" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/rstreics"><img src="https://avatars.githubusercontent.com/u/50323347?v=4" width="100px;" alt=""/><br /><sub><b>rstreics</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=rstreics" title="Code">💻</a> <a href="#infra-rstreics" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/questdb/questdb/commits?author=rstreics" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mariusgheorghies"><img src="https://avatars.githubusercontent.com/u/84250061?v=4" width="100px;" alt=""/><br /><sub><b>mariusgheorghies</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=mariusgheorghies" title="Code">💻</a> <a href="#infra-mariusgheorghies" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/questdb/questdb/commits?author=mariusgheorghies" title="Documentation">📖</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/pswu11"><img src="https://avatars.githubusercontent.com/u/48913707?v=4" width="100px;" alt=""/><br /><sub><b>pswu11</b></sub></a><br /><a href="#content-pswu11" title="Content">🖋</a> <a href="#ideas-pswu11" title="Ideas, Planning, & Feedback">🤔</a> <a href="#design-pswu11" title="Design">🎨</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/insmac"><img src="https://avatars.githubusercontent.com/u/1871646?v=4" width="100px;" alt=""/><br /><sub><b>insmac</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=insmac" title="Code">💻</a> <a href="#ideas-insmac" title="Ideas, Planning, & Feedback">🤔</a> <a href="#design-insmac" title="Design">🎨</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/eugenels"><img src="https://avatars.githubusercontent.com/u/79919431?v=4" width="100px;" alt=""/><br /><sub><b>eugenels</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=eugenels" title="Code">💻</a> <a href="#ideas-eugenels" title="Ideas, Planning, & Feedback">🤔</a> <a href="#maintenance-eugenels" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/bziobrowski"><img src="https://avatars.githubusercontent.com/u/26925920?v=4" width="100px;" alt=""/><br /><sub><b>bziobrowski</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=bziobrowski" title="Code">💻</a> <a href="#projectManagement-bziobrowski" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Zapfmeister"><img src="https://avatars.githubusercontent.com/u/20150586?v=4" width="100px;" alt=""/><br /><sub><b>Zapfmeister</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=Zapfmeister" title="Code">💻</a> <a href="#userTesting-Zapfmeister" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mkaruza"><img src="https://avatars.githubusercontent.com/u/3676457?v=4" width="100px;" alt=""/><br /><sub><b>mkaruza</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=mkaruza" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/DylanDKnight"><img src="https://avatars.githubusercontent.com/u/17187287?v=4" width="100px;" alt=""/><br /><sub><b>DylanDKnight</b></sub></a><br /><a href="#userTesting-DylanDKnight" title="User Testing">📓</a> <a href="https://github.com/questdb/questdb/issues?q=author%3ADylanDKnight" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/enolal826"><img src="https://avatars.githubusercontent.com/u/51820585?v=4" width="100px;" alt=""/><br /><sub><b>enolal826</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=enolal826" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/glasstiger"><img src="https://avatars.githubusercontent.com/u/94906625?v=4" width="100px;" alt=""/><br /><sub><b>glasstiger</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=glasstiger" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://arijus.net"><img src="https://avatars.githubusercontent.com/u/4284659?v=4" width="100px;" alt=""/><br /><sub><b>argshook</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=argshook" title="Code">💻</a> <a href="#ideas-argshook" title="Ideas, Planning, & Feedback">🤔</a> <a href="#design-argshook" title="Design">🎨</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Aargshook" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/amunra"><img src="https://avatars.githubusercontent.com/u/1499096?v=4" width="100px;" alt=""/><br /><sub><b>amunra</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=amunra" title="Code">💻</a> <a href="https://github.com/questdb/questdb/commits?author=amunra" title="Documentation">📖</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Aamunra" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://lamottsjourney.wordpress.com/"><img src="https://avatars.githubusercontent.com/u/66742430?v=4" width="100px;" alt=""/><br /><sub><b>GothamsJoker</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=GothamsJoker" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kocko"><img src="https://avatars.githubusercontent.com/u/862000?v=4" width="100px;" alt=""/><br /><sub><b>kocko</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=kocko" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jerrinot"><img src="https://avatars.githubusercontent.com/u/158619?v=4" width="100px;" alt=""/><br /><sub><b>jerrinot</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=jerrinot" title="Code">💻</a> <a href="#ideas-jerrinot" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Ajerrinot" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://ramiroberrelleza.com"><img src="https://avatars.githubusercontent.com/u/475313?v=4" width="100px;" alt=""/><br /><sub><b>rberrelleza</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=rberrelleza" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Cobalt-27"><img src="https://avatars.githubusercontent.com/u/34511059?v=4" width="100px;" alt=""/><br /><sub><b>Cobalt-27</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=Cobalt-27" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/eschultz"><img src="https://avatars.githubusercontent.com/u/390064?v=4" width="100px;" alt=""/><br /><sub><b>eschultz</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=eschultz" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/xinyi-qiao/"><img src="https://avatars.githubusercontent.com/u/47307374?v=4" width="100px;" alt=""/><br /><sub><b>XinyiQiao</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=XinyiQiao" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://chenquan.me"><img src="https://avatars.githubusercontent.com/u/20042193?v=4" width="100px;" alt=""/><br /><sub><b>terasum</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=terasum" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/hristovdeveloper"><img src="https://avatars.githubusercontent.com/u/3893599?v=4" width="100px;" alt=""/><br /><sub><b>PlamenHristov</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=PlamenHristov" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tris0laris"><img src="https://avatars.githubusercontent.com/u/57298792?v=4" width="100px;" alt=""/><br /><sub><b>tris0laris</b></sub></a><br /><a href="#blog-tris0laris" title="Blogposts">📝</a> <a href="#ideas-tris0laris" title="Ideas, Planning, & Feedback">🤔</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/HeZean"><img src="https://avatars.githubusercontent.com/u/49837965?v=4" width="100px;" alt=""/><br /><sub><b>HeZean</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=HeZean" title="Code">💻</a> <a href="https://github.com/questdb/questdb/issues?q=author%3AHeZean" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/iridess"><img src="https://avatars.githubusercontent.com/u/104518201?v=4" width="100px;" alt=""/><br /><sub><b>iridess</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=iridess" title="Code">💻</a> <a href="https://github.com/questdb/questdb/commits?author=iridess" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/selmanfaruky%C4%B1lmaz/"><img src="https://avatars.githubusercontent.com/u/96119894?v=4" width="100px;" alt=""/><br /><sub><b>selmanfarukyilmaz</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Aselmanfarukyilmaz" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://www.donet5.com"><img src="https://avatars.githubusercontent.com/u/12455385?v=4" width="100px;" alt=""/><br /><sub><b>donet5</b></sub></a><br /><a href="#ideas-donet5" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/questdb/questdb/issues?q=author%3Adonet5" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Zahlii"><img src="https://avatars.githubusercontent.com/u/218582?v=4" width="100px;" alt=""/><br /><sub><b>Zahlii</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3AZahlii" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/salsasepp"><img src="https://avatars.githubusercontent.com/u/4884807?v=4" width="100px;" alt=""/><br /><sub><b>salsasepp</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Asalsasepp" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/EmmettM"><img src="https://avatars.githubusercontent.com/u/4196372?v=4" width="100px;" alt=""/><br /><sub><b>EmmettM</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3AEmmettM" title="Bug reports">🐛</a> <a href="https://github.com/questdb/questdb/commits?author=EmmettM" title="Tests">⚠️</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://allfactors.com"><img src="https://avatars.githubusercontent.com/u/571328?v=4" width="100px;" alt=""/><br /><sub><b>robd003</b></sub></a><br /><a href="#ideas-robd003" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/AllenEdison"><img src="https://avatars.githubusercontent.com/u/46532217?v=4" width="100px;" alt=""/><br /><sub><b>AllenEdison</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3AAllenEdison" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/CSharpDummy"><img src="https://avatars.githubusercontent.com/u/7610502?v=4" width="100px;" alt=""/><br /><sub><b>CSharpDummy</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3ACSharpDummy" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/shimondoodkin"><img src="https://avatars.githubusercontent.com/u/314464?v=4" width="100px;" alt=""/><br /><sub><b>shimondoodkin</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Ashimondoodkin" title="Bug reports">🐛</a> <a href="#ideas-shimondoodkin" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.zsmart.tech/"><img src="https://avatars.githubusercontent.com/u/40519768?v=4" width="100px;" alt=""/><br /><sub><b>huuhait</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Ahuuhait" title="Bug reports">🐛</a> <a href="#ideas-huuhait" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://clickhouse.com/"><img src="https://avatars.githubusercontent.com/u/18581488?v=4" width="100px;" alt=""/><br /><sub><b>alexey-milovidov</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Aalexey-milovidov" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://blog.suconghou.cn"><img src="https://avatars.githubusercontent.com/u/4580719?v=4" width="100px;" alt=""/><br /><sub><b>suconghou</b></sub></a><br /><a href="https://github.com/questdb/questdb/issues?q=author%3Asuconghou" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/allegraharris"><img src="https://avatars.githubusercontent.com/u/89586969?v=4" width="100px;" alt=""/><br /><sub><b>allegraharris</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=allegraharris" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/oliver-daniel"><img src="https://avatars.githubusercontent.com/u/17235417?v=4" width="100px;" alt=""/><br /><sub><b>oliver-daniel</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=oliver-daniel" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kerimsenturk5734"><img src="https://avatars.githubusercontent.com/u/72925170?v=4" width="100px;" alt=""/><br /><sub><b>kerimsenturk5734</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=kerimsenturk5734" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

此項目遵循 [all‑contributors](https://github.com/all-contributors/all-contributors) 規範。  
歡迎所有形式的貢獻！