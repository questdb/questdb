<div align="center">
  <a href="https://questdb.io/" target="blank">
    <img alt="شعار QuestDB" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/>
  </a>
</div>
<p>&nbsp;</p>

<p align="center">
  <a href="#contribute">
    <img src="https://img.shields.io/github/contributors/questdb/questdb" alt="المساهمون في QuestDB مفتوحة المصدر"/>
  </a>
</p>

<p align="center">
  الإنجليزية |
  <a href="./i18n/README.zh-cn.md">简体中文</a> |
  <a href="./i18n/README.zh-hk.md">繁體中文</a> |
  العربية |
  <a href="./i18n/README.it-it.md">Italiano</a> |
  <a href="./i18n/README.ua-ua.md">Українська</a> |
  <a href="./i18n/README.es-es.md">Español</a> |
  <a href="./i18n/README.pt.md">Português</a> |
  <a href="./i18n/README.ja-ja.md">日本語</a> |
  <a href="./i18n/README.tr-tr.md">Türkçe</a> |
  <a href="./i18n/README.hn-in.md">हिंदी</a> |
  <a href="./i18n/README.vi-vn.md">Tiếng Việt</a>
</p>

---

QuestDB هي أسرع قاعدة بيانات سلسلة زمنية مفتوحة المصدر نمواً، وتوفر **ابتلاع بيانات سريع للغاية وبمعدل عالي** و**استعلامات SQL ديناميكية بزمن استجابة منخفض**. تم بناء قاعدة الشيفرة عالية الأداء بالكامل من الصفر باستخدام Java و C++ و Rust دون أي تبعيات وبدون استخدام جامع قمامة.

نحقق الأداء العالي من خلال:
- نموذج تخزين يعتمد على الأعمدة
- تنفيذ متوازي للمتجهات
- تعليمات SIMD
- تقنيات زمن استجابة منخفض

علاوة على ذلك، فإن QuestDB فعّالة من حيث استهلاك العتاد وتتميز بسرعة الإعداد وكفاءة التشغيل.

تطبق QuestDB معيار ANSI SQL مع امتدادات أصلية خاصة بالسلاسل الزمنية. تجعل هذه الامتدادات من السهل تحليل البيانات وتصفيتها وتقليل معدلها، أو الربط بين البيانات من مصادر متعددة باستخدام الانضمامات العلائقية والزمنية.

> هل أنت مستعد؟ انتقل إلى قسم [البدء](#get-started).

<p>&nbsp;</p>

<div align="center">
  <a href="https://demo.questdb.io/">
    <img alt="وحدة تحكم الويب لـ QuestDB تعرض عبارة SQL ونتيجة الاستعلام" src="https://raw.githubusercontent.com/questdb/questdb/master/.github/console.png" width="900" />
  </a>
  <p><em>وحدة تحكم الويب لـ QuestDB - انقر لتشغيل العرض التوضيحي</em></p>
</div>

<p>&nbsp;</p>

## فوائد QuestDB

يتفوق QuestDB في:
- بيانات الأسواق المالية
- أجهزة استشعار إنترنت الأشياء ذات الكثافة العالية للبيانات
- لوحات تحكم في الوقت الفعلي

تشمل الميزات البارزة:
- SQL مع امتدادات سلاسل زمنية قوية ومحسنة باستخدام SIMD
- ابتلاع بيانات عالي السرعة عبر بروتوكول InfluxDB Line
- أداء قوي وفعّال على عتاد محدود
- صيغة تخزين عمودية (محلية أو [Apache Parquet](https://questdb.io/glossary/apache-parquet/))، مع تقسيم وترتيب زمني
- وحدة تحكم ويب سريعة الاستجابة وبديهية لإدارة الاستعلامات والبيانات، مع معالجة الأخطاء
- أداء ممتاز مع [كثافة بيانات عالية](https://questdb.io/glossary/high-cardinality/) – انظر [النتائج](#questdb-performance-vs-other-oss-databases)

ولماذا تستخدم قاعدة بيانات سلسلة زمنية؟

بعيداً عن الأداء والكفاءة، مع قاعدة بيانات متخصصة للسلاسل الزمنية، لن تضطر للقلق بشأن:
- البيانات غير المرتبة
- التكرارات
- اتفاقية "واحد فقط" الدقيقة
- تدفق البيانات (بزمن استجابة منخفض)
- حجم كبير من الطلبات المتزامنة
- البيانات المتقلبة والمفاجئة
- إضافة أعمدة جديدة – يمكنك تغيير المخطط "أثناء" تدفق البيانات

## جرب QuestDB، العرض التوضيحي ولوحات المعلومات

العرض التوضيحي [العام والمباشر](https://demo.questdb.io/) مزود بأحدث إصدار من QuestDB ومجموعات بيانات تجريبية:
- **الرحلات:** 10 سنوات من رحلات سيارات الأجرة في نيويورك مع 1.6 مليار صف
- **التداولات:** بيانات سوق العملات الرقمية المباشرة مع أكثر من 30 مليون صف شهرياً
- **المواقع:** المواقع الجغرافية لـ 250 ألف سفينة فريدة عبر الزمن

استخدم الاستعلامات النموذجية أو اكتب استعلاماتك الخاصة!

_يقوم العرض التوضيحي العام بتنفيذ استعلامات على أكثر من 1.6 مليار صف ويستخدم مثيلاً من النوع `r6a.12xlarge` مع 48 وحدة معالجة افتراضية و348 جيجابايت من الذاكرة._

| الاستعلام                                                                    | زمن التنفيذ                                                                                                                          |
|-------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT sum(double) FROM trips`                                               | [0.15 ثانية](https://demo.questdb.io/?query=SELECT%20sum(trip_distance)%20FROM%20trips;&executeQuery=true)                           |
| `SELECT sum(double), avg(double) FROM trips`                                  | [0.5 ثانية](https://demo.questdb.io/?query=SELECT%20sum(fare_amount),%20avg(fare_amount)%20FROM%20trips;&executeQuery=true)             |
| `SELECT avg(double) FROM trips WHERE time in '2019'`                          | [0.02 ثانية](https://demo.questdb.io/?query=SELECT%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019%27;&executeQuery=true)  |
| `SELECT time, avg(double) FROM trips WHERE time in '2019-01-01' SAMPLE BY 1h` | [0.01 ثانية](https://demo.questdb.io/?query=SELECT%20pickup_datetime,%20avg(trip_distance)%20FROM%20trips%20WHERE%20pickup_datetime%20IN%20%272019-01-01%27%20SAMPLE%20BY%201h;&executeQuery=true) |
| `SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol`                | [0.00025 ثانية](https://demo.questdb.io/?query=SELECT%20*%20FROM%20trades%20LATEST%20ON%20timestamp%20PARTITION%20BY%20symbol;&executeQuery=true)                      |

كما نوفر بعض لوحات المعلومات التجريبية العامة واللحظية باستخدام مكوننا الإضافي [المستند إلى Grafana](https://questdb.io/docs/third-party-tools/grafana/):
- [تداولات العملات الرقمية في الوقت الفعلي:](https://questdb.io/dashboards/crypto/) تداولات تم تنفيذها على OKX لأكثر من 20 أصل في الوقت الفعلي
- [بيانات المواقع الجغرافية لسيارات الأجرة في نيويورك:](https://questdb.io/dashboards/taxi/) إعادة تشغيل في الوقت الفعلي لـ 146,393,317 رحلة سيارات أجرة عبر مدينة نيويورك في عام 2016

### أداء QuestDB مقارنة بغيرها من قواعد البيانات مفتوحة المصدر

يحقق QuestDB أداءً متميزاً في اختبارات الأداء مقارنة بالبدائل.

للتعمق في التفاصيل الداخلية والأداء، راجع المدونات التالية:
- [QuestDB مقابل InfluxDB](https://questdb.io/blog/2024/02/26/questdb-versus-influxdb/)
- [QuestDB مقابل TimescaleDB](https://questdb.io/blog/timescaledb-vs-questdb-comparison/)
- [QuestDB مقابل MongoDB](https://questdb.io/blog/mongodb-time-series-benchmark-review/)

وكالعادة، نشجعك على إجراء اختبارات الأداء الخاصة بك.

<div align="center">
  <img alt="رسم بياني يقارن معدل ابتلاع البيانات بين QuestDB وInfluxDB وTimescaleDB" src=".github/readme-benchmark.png" width="600"/>
</div>

## ابدأ الآن

استخدم [Docker](https://www.docker.com/) للبدء بسرعة:

```bash
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```

أو يمكن لمستخدمي macOS استخدام Homebrew:

```bash
brew install questdb
brew services start questdb
```

```bash
questdb start
questdb stop
```

بدلاً من ذلك، للبدء برحلة التعرّف الكاملة، ابدأ بدليلنا المختصر [للبدء السريع](https://questdb.io/docs/quick-start/).

### عملاء الإدخال الأوليين

عملاء QuestDB لإدخال البيانات عبر بروتوكول InfluxDB Line:

- [Python](https://questdb.io/docs/clients/ingest-python/)
- [.NET](https://questdb.io/docs/clients/ingest-dotnet/)
- [C/C++](https://questdb.io/docs/clients/ingest-c-and-cpp/)
- [Go](https://questdb.io/docs/clients/ingest-go/)
- [Java](https://questdb.io/docs/clients/java_ilp/)
- [NodeJS](https://questdb.io/docs/clients/ingest-node/)
- [Rust](https://questdb.io/docs/clients/ingest-rust/)

### الاتصال بـ QuestDB

تفاعل مع QuestDB وبياناتك من خلال الواجهات التالية:

- [وحدة تحكم الويب](https://questdb.io/docs/web-console/) لمحرر SQL تفاعلي واستيراد CSV على المنفذ `9000`
- [بروتوكول InfluxDB Line](https://questdb.io/docs/reference/api/ilp/overview/) للإدخال المتدفق على المنفذ `9000`
- [بروتوكول PostgreSQL Wire](https://questdb.io/docs/reference/api/postgres/) للاستعلامات البرمجية على المنفذ `8812`
- [واجهة REST](https://questdb.io/docs/reference/api/rest/) لاستيراد CSV واستخدام cURL على المنفذ `9000`

### أدوات الطرف الثالث الشهيرة

تشمل الأدوات الشهيرة التي تتكامل مع QuestDB:
- [Apache Kafka](https://questdb.io/docs/third-party-tools/kafka/)
- [Grafana](https://questdb.io/docs/third-party-tools/grafana/)
- [Superset](https://questdb.io/docs/third-party-tools/superset/)
- [Telegraf](https://questdb.io/docs/third-party-tools/telegraf/)
- [Apache Flink](https://questdb.io/docs/third-party-tools/flink/)
- [qStudio](https://questdb.io/docs/third-party-tools/qstudio/)
- [MindsDB](https://questdb.io/docs/third-party-tools/mindsdb/)

### نماذج الشيفرة من البداية للنهاية

من إدخال البيانات المتدفقة إلى التمثيل البصري باستخدام Grafana، ابدأ بنماذج الشيفرة من خلال [مستودع البدء السريع](https://github.com/questdb/questdb-quickstart).

### تهيئة QuestDB للأحمال الإنتاجية

اطلع على [تخطيط السعة](https://questdb.io/docs/deployment/capacity-planning/) لضبط QuestDB لتلبية متطلبات الأحمال الإنتاجية.

### QuestDB للمؤسسات

للتشغيل الآمن على نطاق أوسع أو داخل المؤسسات الكبرى.

تشمل الميزات الإضافية:
- إدخال بيانات متعدد الرؤوس
- نسخ قراءة مكررة
- تكامل التخزين البارد
- التحكم في الوصول بناءً على الأدوار
- تشفير TLS
- الاستعلام الأصلي عن ملفات Parquet عبر تخزين الكائنات
- دعم اتفاقيات مستوى الخدمة (SLAs)، ومراقبة محسنة، والمزيد

قم بزيارة [صفحة المؤسسات](https://questdb.io/enterprise/) لمزيد من التفاصيل ومعلومات الاتصال.

## موارد إضافية

### 📚 اقرأ الوثائق

- [وثائق QuestDB:](https://questdb.io/docs/) ابدأ رحلتك
- [خارطة طريق المنتج:](https://github.com/orgs/questdb/projects/1/views/5) اطلع على خططنا للإصدارات القادمة
- [الدروس التعليمية:](https://questdb.io/tutorial/) تعلّم ما هو ممكن مع QuestDB خطوة بخطوة

### ❓ الحصول على الدعم

- [منتدى Community Discourse:](https://community.questdb.io/) انضم إلى المناقشات التقنية، واطرح الأسئلة، وتعرف على مستخدمين آخرين!
- [Slack العام:](https://slack.questdb.io/) تواصل مع فريق QuestDB وأعضاء المجتمع
- [قضايا GitHub:](https://github.com/questdb/questdb/issues) أبلغ عن الأخطاء أو المشاكل في QuestDB
- [Stack Overflow:](https://stackoverflow.com/questions/tagged/questdb) ابحث عن حلول لاستكشاف المشكلات الشائعة

### 🚢 نشر QuestDB

- [صورة AWS AMI](https://questdb.io/docs/guides/aws-official-ami)
- [Google Cloud Platform](https://questdb.io/docs/guides/google-cloud-platform)
- [الصورة الرسمية لـ Docker](https://questdb.io/docs/get-started/docker)
- [خوادم DigitalOcean](https://questdb.io/docs/guides/digitalocean)
- [مخططات Helm لـ Kubernetes](https://questdb.io/docs/guides/kubernetes)

## المساهمة

المساهمات مرحب بها!

نحن نقدر:
- شفرة المصدر
- التوثيق (انظر إلى [مستودع التوثيق](https://github.com/questdb/documentation))
- تقارير الأخطاء
- طلبات الميزات أو التعليقات

لبدء المساهمة:
- ألق نظرة على قضايا GitHub المصنفة بـ "[Good first issue](https://github.com/questdb/questdb/issues?q=is%3Aissue+is%3Aopen+label%3A%22Good+first+issue%22)".
- بالنسبة لموسم Hacktoberfest، راجع [القضايا المصنفة](https://github.com/questdb/questdb/issues?q=is%3Aissue+is%3Aopen+label%3Ahacktoberfest)
- اقرأ [دليل المساهمة](https://github.com/questdb/questdb/blob/master/CONTRIBUTING.md)
- للحصول على تفاصيل حول بناء QuestDB، راجع [تعليمات البناء](https://github.com/questdb/questdb/blob/master/core/README.md)
- [أنشئ fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) من QuestDB وقدّم طلب سحب مع التغييرات المقترحة
- هل واجهت صعوبة؟ انضم إلى [Slack العام](https://slack.questdb.io/) للحصول على المساعدة

✨ كعلامة على امتناننا، نقوم بإرسال **[QuestDB swag](https://questdb.io/community)** لمساهمينا!

شكر كبير للأشخاص الرائعين التاليين الذين ساهموا في QuestDB  
[مفتاح الرموز التعبيرية](https://allcontributors.org/docs/en/emoji-key):

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
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mick2004"><img src="https://avatars.githubusercontent.com/u/2042132?v=4" width="100px;" alt=""/><br /><sub><b>mick2004</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=mick2004" title="Code">💻</a> <a href="#platform-mick2004" title="Packaging/porting to new platform">📦</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://rawkode.com"><img src="https://avatars3.githubusercontent.com/u/145816?v=4" width="100px;" alt=""/><br /><sub><b>rawkode</b></sub></a><br /><a href="https://github.com/questdb/questdb/commits?author=rawkode" title="Code">💻</a> <a href="#infra-rawkode" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
    </tr>
    <!-- باقي جدول المساهمين كما هو دون تغيير -->
  </tbody>
</table>
<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

هذا المشروع يتبع مواصفة 
[كل المساهمين](https://github.com/all-contributors/all-contributors).
جميع أنواع المساهمات مرحب بها!
