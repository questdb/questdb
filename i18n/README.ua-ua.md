<div align="center">
  <a href="https://questdb.io/" target="blank">
    <img alt="QuestDB Логотип" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/>
  </a>
</div>
<p>&nbsp;</p>

<p align="center">
  <a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="Slack-канал спільноти QuestDB"/>
  </a>
  <a href="#contribute">
    <img src="https://img.shields.io/github/contributors/questdb/questdb" alt="Контриб’ютори QuestDB"/>
  </a>
  <a href="https://search.maven.org/search?q=g:org.questdb">
    <img src="https://img.shields.io/maven-central/v/org.questdb/questdb" alt="QuestDB на Apache Maven"/>
  </a>
</p>

<p align="center">
  <a href="https://github.com/questdb/questdb">English</a> |
  <a href="README.zh-cn.md">简体中文</a> |
  <a href="README.zh-hk.md">繁體中文</a> |
  <a href="README.ar-dz.md">العربية</a> |
  <a href="README.it-it.md">Italiano</a> |
  Українська |
  <a href="README.es-es.md">Español</a> |
  <a href="README.pt.md">Português</a> |
  <a href="README.ja-ja.md">日本語</a> |
  <a href="README.tr-tr.md">Türkçe</a> |
  <a href="README.hn-in.md">हिंदी</a> |
  <a href="README.vi-vn.md">Tiếng Việt</a>
</p>

# QuestDB

QuestDB — це найшвидше зростаюча база даних часових рядів з відкритим кодом, яка забезпечує **блискавично швидке надходження даних із високою пропускною здатністю** та **динамічні SQL-запити з низькою затримкою**. Весь високопродуктивний кодовий базис створено з нуля мовами Java, C++ та Rust, без зовнішніх залежностей та без використання збирача сміття.

Ми досягаємо високої продуктивності завдяки:
- колонковому (column-oriented) моделі зберігання даних,
- паралельному векторному виконанню,
- використанню SIMD-інструкцій,
- та низьколатентним (low-latency) технологіям.

Крім того, QuestDB ефективно використовує апаратні засоби, швидко налаштовується і забезпечує оперативну ефективність.

QuestDB реалізує ANSI SQL із власними нативними розширеннями для роботи з часовими рядами. Ці розширення значно спрощують аналіз, фільтрацію та зведення даних, а також дозволяють легко корелювати дані з різних джерел за допомогою реляційних та часових join’ів.

> Готові розпочати? Перейдіть до розділу [Почніть зараз](#почніть-зараз).

<p>&nbsp;</p>

<div align="center">
  <a href="https://demo.questdb.io/">
    <img alt="QuestDB Web Console, яка показує SQL-вираз і результати запиту" src="https://raw.githubusercontent.com/questdb/questdb/master/.github/console.png" width="900" />
  </a>
  <p><em>QuestDB Web Console – натисніть, щоб запустити демо</em></p>
</div>

<p>&nbsp;</p>

## Переваги QuestDB

QuestDB відмінно підходить для:
- фінансових ринкових даних,
- IoT-даних з високою кардинальністю,
- реального часу аналітики та створення дашбордів.

Основні можливості:
- SQL із потужними, SIMD-оптимізованими розширеннями для часових рядів,
- високошвидкісний збір даних за допомогою InfluxDB Line Protocol,
- сильна та ефективна продуктивність навіть на обмеженому обладнанні,
- нативний або [Apache Parquet](https://questdb.io/glossary/apache-parquet/)-сумісний формат зберігання даних, який розділяється і сортується за часом,
- інтуїтивно зрозуміла та швидка Web Console для управління запитами та даними з обробкою помилок,
- відмінна продуктивність при високій кардинальності даних – див. [benchmark](#questdb-performance-vs-other-oss-databases).

### Чому використовувати базу даних часових рядів?

Окрім високої продуктивності та ефективності, спеціалізована база даних часових рядів дозволяє не турбуватися про:
- дані, що надходять не за порядком,
- дублікати,
- вимогу "рівно один" (exactly one),
- стрімінгові дані (низька затримка),
- велику кількість одночасних запитів,
- нестабільні або "бурхливі" дані,
- можливість додавання нових стовпців без зупинки потокового надходження даних.

## Спробуйте QuestDB

Ми надаємо [онлайн демо](https://demo.questdb.io/), яке працює на останній версії QuestDB та містить зразки даних:
- **Trips:** 10 років поїздок таксі у Нью-Йорку з 1,6 мільярдами рядків,
- **Trades:** реальні дані ринку криптовалют з понад 30 млн. рядків щомісяця,
- **Pos:** геолокації 250 тис. унікальних кораблів протягом часу.

Ви можете використовувати наведені приклади запитів або написати свої.

_Примітка: Демонстраційна система обробляє понад 1,6 млрд. рядків і використовує інстанс `r6a.12xlarge` з 48 vCPU та 348 GB оперативної пам’яті._

| Запит                                                                         | Час виконання                                                                                                                                                                                     |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT sum(double) FROM trips`                                               | [0.15 secs](https://demo.questdb.io/?query=SELECT%20sum(trip_distance)%20FROM%20trips;&executeQuery=true)                                                                                         |
| `SELECT sum(double), avg(double) FROM trips`                                  | [0.5 secs](https://demo.questdb.io/?query=SELECT%20sum(fare_amount),%20avg(fare_amount)%20FROM%20trips;&executeQuery=true)                                                                            |
| `SELECT avg(double) FROM trips WHERE time in '2019'`                          | [0.02 secs](https://demo.questdb.io/?query=SELECT%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019%27;&executeQuery=true)                                           |
| `SELECT time, avg(double) FROM trips WHERE time in '2019-01-01' SAMPLE BY 1h` | [0.01 secs](https://demo.questdb.io/?query=SELECT%20pickup_datetime,%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019-01-01%27%20SAMPLE%20BY%201h;&executeQuery=true) |
| `SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol`                | [0.00025 secs](https://demo.questdb.io/?query=SELECT%20*%20FROM%20trades%20LATEST%20ON%20timestamp%20PARTITION%20BY%20symbol;&executeQuery=true)                                               |

## QuestDB Performance vs. Other OSS Databases

QuestDB демонструє виняткову продуктивність у benchmark-тестах у порівнянні з іншими відкритими базами даних.

Для детального ознайомлення з внутрішніми деталями та продуктивністю перегляньте такі блоги:
- [QuestDB vs InfluxDB](https://questdb.io/blog/2024/02/26/questdb-versus-influxdb/)
- [QuestDB vs TimescaleDB](https://questdb.io/blog/timescaledb-vs-questdb-comparison/)
- [QuestDB vs MongoDB](https://questdb.io/blog/mongodb-time-series-benchmark-review/)

Як завжди, ми рекомендуємо вам проводити власні benchmark-тести.

<div align="center">
  <img alt="Діаграма, що порівнює максимальну пропускну здатність QuestDB, InfluxDB та TimescaleDB" src="https://user-images.githubusercontent.com/91843271/197382161-e5f3f5b0-18bf-439a-94e4-83ab4bf91d7c.png" width="600"/>
</div>

## Почніть зараз

### Встановіть QuestDB

Для швидкого запуску QuestDB використовуйте [Docker](https://www.docker.com/):

```bash
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```

Користувачі macOS можуть скористатися Homebrew:

```bash
brew install questdb
brew services start questdb

questdb start   # Запуск QuestDB
questdb stop    # Зупинка QuestDB
```

Для повноцінного ознайомлення перегляньте наш [Quick Start Guide](https://questdb.io/docs/quick-start/).

### Офіційні клієнти для інжестії

QuestDB надає офіційні клієнти для надходження даних через InfluxDB Line Protocol:

- [Python](https://questdb.io/docs/clients/ingest-python/)
- [.NET](https://questdb.io/docs/clients/ingest-dotnet/)
- [C/C++](https://questdb.io/docs/clients/ingest-c-and-cpp/)
- [Go](https://questdb.io/docs/clients/ingest-go/)
- [Java](https://questdb.io/docs/clients/java_ilp/)
- [NodeJS](https://questdb.io/docs/clients/ingest-node/)
- [Rust](https://questdb.io/docs/clients/ingest-rust/)

### Підключіться до QuestDB

Ви можете взаємодіяти з QuestDB та вашими даними за допомогою наступних інтерфейсів:

- [Web Console](https://questdb.io/docs/web-console/) – інтерактивний SQL-редактор і імпорт CSV на порті `9000`
- [InfluxDB Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/) – для потокової інжестії даних на порті `9000`
- [PostgreSQL Wire Protocol](https://questdb.io/docs/reference/api/postgres/) – для програмних запитів на порті `8812`
- [REST API](https://questdb.io/docs/reference/api/rest/) – для імпорту CSV і використання з cURL на порті `9000`

### Інтеграція з інструментами третіх сторін

QuestDB інтегрується з багатьма популярними інструментами:

- [Apache Kafka](https://questdb.io/docs/third-party-tools/kafka/)
- [Grafana](https://questdb.io/docs/third-party-tools/grafana/)
- [Superset](https://questdb.io/docs/third-party-tools/superset/)
- [Telegraf](https://questdb.io/docs/third-party-tools/telegraf/)
- [Apache Flink](https://questdb.io/docs/third-party-tools/flink/)
- [qStudio](https://questdb.io/docs/third-party-tools/qstudio/)
- [MindsDB](https://questdb.io/docs/third-party-tools/mindsdb/)

### End-to-End Scaffold

Якщо ви бажаєте отримати повний досвід – від потокової інжестії даних до візуалізації з Grafana – перегляньте наш [Quickstart Repository](https://github.com/questdb/questdb-quickstart).

### Налаштуйте QuestDB для робочих навантажень у виробництві

Перегляньте [Capacity Planning Guide](https://questdb.io/docs/deployment/capacity-planning/), щоб оптимізувати QuestDB для виробничих навантажень.

### QuestDB Enterprise

Для безпечної роботи у великих масштабах або у великих організаціях QuestDB Enterprise пропонує додаткові функції:

- Мульти-примарна інжестія
- Реплікацію для читання
- Інтеграцію з холодним сховищем
- Контроль доступу на основі ролей
- TLS-шифрування
- Можливість нативного запиту Parquet файлів через object storage
- Підтримку SLA, розширений моніторинг та інше

Детальніше дивіться на [Enterprise сторінці](https://questdb.io/enterprise/).

## Додаткові ресурси

### 📚 Читайте документацію

- [Документація QuestDB:](https://questdb.io/docs/introduction/) – дізнайтеся, як запускати та налаштовувати QuestDB.
- [Підручники:](https://questdb.io/tutorial/) – крок за кроком дізнайтеся, що можна зробити з QuestDB.
- [Дорожня карта продукту:](https://github.com/questdb/questdb/projects) – перегляньте наші плани на майбутні релізи.

### ❓ Отримайте підтримку

- [Community Slack:](https://slack.questdb.io) – приєднуйтеся до технічних дискусій, ставте запитання та знайомтеся з іншими користувачами!
- [GitHub Issues:](https://github.com/questdb/questdb/issues) – повідомляйте про баги чи проблеми з QuestDB.
- [GitHub Discussions:](https://github.com/questdb/questdb/discussions) – пропонуйте нові функції або демонструйте свої проекти.
- [Stack Overflow:](https://stackoverflow.com/questions/tagged/questdb) – шукайте загальні рішення для вирішення проблем.

### 🚢 Розгортання QuestDB

- [AWS AMI](https://questdb.io/docs/guides/aws-official-ami)
- [Google Cloud Platform](https://questdb.io/docs/guides/google-cloud-platform)
- [Офіційний Docker-образ](https://questdb.io/docs/get-started/docker)
- [DigitalOcean Droplets](https://questdb.io/docs/guides/digitalocean)
- [Kubernetes Helm Charts](https://questdb.io/docs/guides/kubernetes)

## Внесіть свій внесок

Ми завжди раді приймати внески до проєкту, незалежно від того, чи це код, документація, звіти про помилки, запити на нові функції чи зворотній зв’язок.

### Як зробити внесок

- Ознайомтеся з issue на GitHub, позначеними як "[Good first issue](https://github.com/questdb/questdb/issues?q=is%3Aissue+is%3Aopen+label%3A%22Good+first+issue%22)".
- Прочитайте [керівництво для контриб’юторів](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md).
- Для деталей про збірку QuestDB перегляньте [build instructions](https://github.com/questdb/questdb/blob/master/core/README.md).
- [Створіть форк](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) QuestDB та надішліть pull request із запропонованими змінами.
- Якщо виникнуть труднощі, приєднуйтеся до нашого [public Slack](https://slack.questdb.io/) для отримання допомоги.

✨ На знак нашої вдячності, ми надсилаємо **QuestDB swag** нашим контриб’юторам.  
[Отримайте свій swag тут](https://questdb.io/community).

Велика подяка наступним чудовим людям, які зробили свій внесок у QuestDB:  
([emoji key](https://allcontributors.org/docs/en/emoji-key)).

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

Below is the complete updated Ukrainian README file in Markdown. You can copy and paste the text into your repository (for example, as `README.ua-ua.md`):

```markdown
<div align="center">
  <a href="https://questdb.io/" target="blank">
    <img alt="QuestDB Логотип" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/>
  </a>
</div>
<p>&nbsp;</p>

<p align="center">
  <a href="https://slack.questdb.io">
    <img src="https://slack.questdb.io/badge.svg" alt="Slack-канал спільноти QuestDB"/>
  </a>
  <a href="#contribute">
    <img src="https://img.shields.io/github/contributors/questdb/questdb" alt="Контриб’ютори QuestDB"/>
  </a>
  <a href="https://search.maven.org/search?q=g:org.questdb">
    <img src="https://img.shields.io/maven-central/v/org.questdb/questdb" alt="QuestDB на Apache Maven"/>
  </a>
</p>

<p align="center">
  [English](https://github.com/questdb/questdb) | [简体中文](README.zh-cn.md) | [繁體中文](README.zh-hk.md) | [العربية](README.ar-dz.md) | [Italiano](README.it-it.md) | [Українська](README.ua-ua.md) | [Español](README.es-es.md) | [Português](README.pt.md) | [日本語](README.ja-ja.md) | [Türkçe](README.tr-tr.md) | [हिंदी](README.hn-in.md) | [Tiếng Việt](README.vi-vn.md)
</p>

# QuestDB

QuestDB — це найшвидше зростаюча база даних часових рядів з відкритим кодом, яка забезпечує **блискавично швидке надходження даних із високою пропускною здатністю** та **динамічні SQL-запити з низькою затримкою**. Весь високопродуктивний кодовий базис створено з нуля мовами Java, C++ та Rust, без зовнішніх залежностей та без використання збирача сміття.

Ми досягаємо високої продуктивності завдяки:
- колонковому (column-oriented) моделі зберігання даних,
- паралельному векторному виконанню,
- використанню SIMD-інструкцій,
- та низьколатентним (low-latency) технологіям.

Крім того, QuestDB ефективно використовує апаратні засоби, швидко налаштовується і забезпечує оперативну ефективність.

QuestDB реалізує ANSI SQL із власними нативними розширеннями для роботи з часовими рядами. Ці розширення значно спрощують аналіз, фільтрацію та зведення даних, а також дозволяють легко корелювати дані з різних джерел за допомогою реляційних та часових join’ів.

> Готові розпочати? Перейдіть до розділу [Почніть зараз](#почніть-зараз).

<p>&nbsp;</p>

<div align="center">
  <a href="https://demo.questdb.io/">
    <img alt="QuestDB Web Console, яка показує SQL-вираз і результати запиту" src="https://raw.githubusercontent.com/questdb/questdb/master/.github/console.png" width="900" />
  </a>
  <p><em>QuestDB Web Console – натисніть, щоб запустити демо</em></p>
</div>

<p>&nbsp;</p>

## Переваги QuestDB

QuestDB відмінно підходить для:
- фінансових ринкових даних,
- IoT-даних з високою кардинальністю,
- реального часу аналітики та створення дашбордів.

Основні можливості:
- SQL із потужними, SIMD-оптимізованими розширеннями для часових рядів,
- високошвидкісний збір даних за допомогою InfluxDB Line Protocol,
- сильна та ефективна продуктивність навіть на обмеженому обладнанні,
- нативний або [Apache Parquet](https://questdb.io/glossary/apache-parquet/)-сумісний формат зберігання даних, який розділяється і сортується за часом,
- інтуїтивно зрозуміла та швидка Web Console для управління запитами та даними з обробкою помилок,
- відмінна продуктивність при високій кардинальності даних – див. [benchmark](#questdb-performance-vs-other-oss-databases).

### Чому використовувати базу даних часових рядів?

Окрім високої продуктивності та ефективності, спеціалізована база даних часових рядів дозволяє не турбуватися про:
- дані, що надходять не за порядком,
- дублікати,
- вимогу "рівно один" (exactly one),
- стрімінгові дані (низька затримка),
- велику кількість одночасних запитів,
- нестабільні або "бурхливі" дані,
- можливість додавання нових стовпців без зупинки потокового надходження даних.

## Спробуйте QuestDB

Ми надаємо [онлайн демо](https://demo.questdb.io/), яке працює на останній версії QuestDB та містить зразки даних:
- **Trips:** 10 років поїздок таксі у Нью-Йорку з 1,6 мільярдами рядків,
- **Trades:** реальні дані ринку криптовалют з понад 30 млн. рядків щомісяця,
- **Pos:** геолокації 250 тис. унікальних кораблів протягом часу.

Ви можете використовувати наведені приклади запитів або написати свої.

_Примітка: Демонстраційна система обробляє понад 1,6 млрд. рядків і використовує інстанс `r6a.12xlarge` з 48 vCPU та 348 GB оперативної пам’яті._

| Запит                                                                         | Час виконання                                                                                                                                                                                     |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT sum(double) FROM trips`                                               | [0.15 secs](https://demo.questdb.io/?query=SELECT%20sum(trip_distance)%20FROM%20trips;&executeQuery=true)                                                                                         |
| `SELECT sum(double), avg(double) FROM trips`                                  | [0.5 secs](https://demo.questdb.io/?query=SELECT%20sum(fare_amount),%20avg(fare_amount)%20FROM%20trips;&executeQuery=true)                                                                            |
| `SELECT avg(double) FROM trips WHERE time in '2019'`                          | [0.02 secs](https://demo.questdb.io/?query=SELECT%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019%27;&executeQuery=true)                                           |
| `SELECT time, avg(double) FROM trips WHERE time in '2019-01-01' SAMPLE BY 1h` | [0.01 secs](https://demo.questdb.io/?query=SELECT%20pickup_datetime,%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019-01-01%27%20SAMPLE%20BY%201h;&executeQuery=true) |
| `SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol`                | [0.00025 secs](https://demo.questdb.io/?query=SELECT%20*%20FROM%20trades%20LATEST%20ON%20timestamp%20PARTITION%20BY%20symbol;&executeQuery=true)                                               |

## QuestDB Performance vs. Other OSS Databases

QuestDB демонструє виняткову продуктивність у benchmark-тестах у порівнянні з іншими відкритими базами даних.

Для детального ознайомлення з внутрішніми деталями та продуктивністю перегляньте такі блоги:
- [QuestDB vs InfluxDB](https://questdb.io/blog/2024/02/26/questdb-versus-influxdb/)
- [QuestDB vs TimescaleDB](https://questdb.io/blog/timescaledb-vs-questdb-comparison/)
- [QuestDB vs MongoDB](https://questdb.io/blog/mongodb-time-series-benchmark-review/)

Як завжди, ми рекомендуємо вам проводити власні benchmark-тести.

<div align="center">
  <img alt="Діаграма, що порівнює максимальну пропускну здатність QuestDB, InfluxDB та TimescaleDB" src="https://user-images.githubusercontent.com/91843271/197382161-e5f3f5b0-18bf-439a-94e4-83ab4bf91d7c.png" width="600"/>
</div>

## Почніть зараз

### Встановіть QuestDB

Для швидкого запуску QuestDB використовуйте [Docker](https://www.docker.com/):

```bash
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```

Користувачі macOS можуть скористатися Homebrew:

```bash
brew install questdb
brew services start questdb

questdb start   # Запуск QuestDB
questdb stop    # Зупинка QuestDB
```

Для повноцінного ознайомлення перегляньте наш [Quick Start Guide](https://questdb.io/docs/quick-start/).

### Офіційні клієнти для інжестії

QuestDB надає офіційні клієнти для надходження даних через InfluxDB Line Protocol:

- [Python](https://questdb.io/docs/clients/ingest-python/)
- [.NET](https://questdb.io/docs/clients/ingest-dotnet/)
- [C/C++](https://questdb.io/docs/clients/ingest-c-and-cpp/)
- [Go](https://questdb.io/docs/clients/ingest-go/)
- [Java](https://questdb.io/docs/clients/java_ilp/)
- [NodeJS](https://questdb.io/docs/clients/ingest-node/)
- [Rust](https://questdb.io/docs/clients/ingest-rust/)

### Підключіться до QuestDB

Ви можете взаємодіяти з QuestDB та вашими даними за допомогою наступних інтерфейсів:

- [Web Console](https://questdb.io/docs/web-console/) – інтерактивний SQL-редактор і імпорт CSV на порті `9000`
- [InfluxDB Line Protocol](https://questdb.io/docs/reference/api/ilp/overview/) – для потокової інжестії даних на порті `9000`
- [PostgreSQL Wire Protocol](https://questdb.io/docs/reference/api/postgres/) – для програмних запитів на порті `8812`
- [REST API](https://questdb.io/docs/reference/api/rest/) – для імпорту CSV і використання з cURL на порті `9000`

### Інтеграція з інструментами третіх сторін

QuestDB інтегрується з багатьма популярними інструментами:

- [Apache Kafka](https://questdb.io/docs/third-party-tools/kafka/)
- [Grafana](https://questdb.io/docs/third-party-tools/grafana/)
- [Superset](https://questdb.io/docs/third-party-tools/superset/)
- [Telegraf](https://questdb.io/docs/third-party-tools/telegraf/)
- [Apache Flink](https://questdb.io/docs/third-party-tools/flink/)
- [qStudio](https://questdb.io/docs/third-party-tools/qstudio/)
- [MindsDB](https://questdb.io/docs/third-party-tools/mindsdb/)

### End-to-End Scaffold

Якщо ви бажаєте отримати повний досвід – від потокової інжестії даних до візуалізації з Grafana – перегляньте наш [Quickstart Repository](https://github.com/questdb/questdb-quickstart).

### Налаштуйте QuestDB для робочих навантажень у виробництві

Перегляньте [Capacity Planning Guide](https://questdb.io/docs/deployment/capacity-planning/), щоб оптимізувати QuestDB для виробничих навантажень.

### QuestDB Enterprise

Для безпечної роботи у великих масштабах або у великих організаціях QuestDB Enterprise пропонує додаткові функції:

- Мульти-примарна інжестія
- Реплікацію для читання
- Інтеграцію з холодним сховищем
- Контроль доступу на основі ролей
- TLS-шифрування
- Можливість нативного запиту Parquet файлів через object storage
- Підтримку SLA, розширений моніторинг та інше

Детальніше дивіться на [Enterprise сторінці](https://questdb.io/enterprise/).

## Додаткові ресурси

### 📚 Читайте документацію

- [Документація QuestDB:](https://questdb.io/docs/introduction/) – дізнайтеся, як запускати та налаштовувати QuestDB.
- [Підручники:](https://questdb.io/tutorial/) – крок за кроком дізнайтеся, що можна зробити з QuestDB.
- [Дорожня карта продукту:](https://github.com/questdb/questdb/projects) – перегляньте наші плани на майбутні релізи.

### ❓ Отримайте підтримку

- [Community Slack:](https://slack.questdb.io) – приєднуйтеся до технічних дискусій, ставте запитання та знайомтеся з іншими користувачами!
- [GitHub Issues:](https://github.com/questdb/questdb/issues) – повідомляйте про баги чи проблеми з QuestDB.
- [GitHub Discussions:](https://github.com/questdb/questdb/discussions) – пропонуйте нові функції або демонструйте свої проекти.
- [Stack Overflow:](https://stackoverflow.com/questions/tagged/questdb) – шукайте загальні рішення для вирішення проблем.

### 🚢 Розгортання QuestDB

- [AWS AMI](https://questdb.io/docs/guides/aws-official-ami)
- [Google Cloud Platform](https://questdb.io/docs/guides/google-cloud-platform)
- [Офіційний Docker-образ](https://questdb.io/docs/get-started/docker)
- [DigitalOcean Droplets](https://questdb.io/docs/guides/digitalocean)
- [Kubernetes Helm Charts](https://questdb.io/docs/guides/kubernetes)

## Внесіть свій внесок

Ми завжди раді приймати внески до проєкту, незалежно від того, чи це код, документація, звіти про помилки, запити на нові функції чи зворотній зв’язок.

### Як зробити внесок

- Ознайомтеся з issue на GitHub, позначеними як "[Good first issue](https://github.com/questdb/questdb/issues?q=is%3Aissue+is%3Aopen+label%3A%22Good+first+issue%22)".
- Прочитайте [керівництво для контриб’юторів](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md).
- Для деталей про збірку QuestDB перегляньте [build instructions](https://github.com/questdb/questdb/blob/master/core/README.md).
- [Створіть форк](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) QuestDB та надішліть pull request із запропонованими змінами.
- Якщо виникнуть труднощі, приєднуйтеся до нашого [public Slack](https://slack.questdb.io/) для отримання допомоги.

✨ На знак нашої вдячності, ми надсилаємо **QuestDB swag** нашим контриб’юторам.  
[Отримайте свій swag тут](https://questdb.io/community).

Велика подяка наступним чудовим людям, які зробили свій внесок у QuestDB:  
([emoji key](https://allcontributors.org/docs/en/emoji-key)).

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- (Повна таблиця контриб’юторів буде тут, без змін) -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

Цей проєкт відповідає специфікації [all‑contributors](https://github.com/all-contributors/all-contributors).  
Будь-які внески вітаються!
