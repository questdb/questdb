# GROUP BY 内部的串行与并行 Stream Aggregation

实现基线：`d411adea25d6a7500cadb5c73c2146c2b68882a1`。本文描述本次代码的实际行为；性能数据来自同一基线与本次实现的对照，不能外推为所有 SAMPLE BY 查询的加速倍数。

## 1. 决策与范围

Stream aggregation 是 `generateSelectGroupBy()` 的一种执行策略。优化器证明完整分组键连续、聚合状态可以安全复用后，使用 stream；否则保留原有执行路径。串行和并行都有实现，EXPLAIN 可以区分两者。不增加 SQL 语法、配置项、用户 hint 或通用表达式证明框架。

calendar-aligned `SAMPLE BY` 仍由现有优化器改写成以 `timestamp_floor_utc(...)` 为键的 GROUP BY。专用 SAMPLE BY 原本就有按桶执行的串行游标；关闭并行后，被改写的 SAMPLE BY 以前仍可能使用串行 hash。因此，本次优化作用于 GROUP BY，不替换全部 SAMPLE BY 实现。

[PR 7706](https://github.com/questdb/questdb/pull/7706) 的时间桶缓存与本次优化可以共存。桶缓存减少键函数计算；stream 进一步省去每行 hash/probe，并让并行 reducer 对一段同组数据调用已有的批量聚合。

| 已实现 | 保留原执行路径 |
| --- | --- |
| 一个有序的 timestamp、long 或 int 分组键 | 多键分组，包括 `(时间桶, symbol)` |
| 物理 timestamp 正向扫描及可验证的投影、过滤包装 | 反向扫描、任意 Sort/Join/VirtualRecord 的顺序推导 |
| 白名单内的 timestamp 保序变换；`long_sequence().x` | 多层函数链、任意算术表达式的单调性证明 |
| 状态完整存放在 MapValue 的聚合函数 | 状态包含外部指针、集合、数组或字符串存储的聚合 |
| 串行过滤；并行无残余过滤的 page frames | 并行残余过滤，包括稀疏行号及 late materialization |
| `FILL(NONE)` / 无填充 | 实际填充和要求共享物化游标的计划 |

部分键连续时，在时间桶内做 hash 是另一个执行策略。本次没有实现它，也不把它算进 stream 的支持范围或性能收益。`symbol` 作为 `first/last` 的聚合参数可以支持；`symbol` 作为第二个分组键仍使用原路径。

## 2. 正确性前提：证明分组连续

### 2.1 输入顺序必须来自实际执行器

判定集中在 [GroupByUtils](../core/src/main/java/io/questdb/griffin/engine/groupby/GroupByUtils.java)，没有另建 `StreamGroupByUtils`。

`PageFrameRecordCursorFactory.isAscendingTimestamp()` 同时检查：

1. 分区扫描 metadata 中确实存在 designated timestamp。
2. 查询列经 `columnIndexes` 映射后就是这一个物理列。
3. 分区按正向顺序扫描，行游标是顺序实体扫描且不使用索引。

仅有 `getScanDirection() == FORWARD` 或查询 metadata 的 timestamp 标记不够。例如 `FROM trades TIMESTAMP(other)` 可以把普通、乱序、甚至包含 NULL 的列标为查询 timestamp。按这个标记启用 stream 会把重复分组拆开；专项测试覆盖了这种情况及列投影后的情况。

`SelectedRecordCursorFactory` 按 `columnCrossIndex` 映射列号；已知保持行序的过滤包装继续检查其底层输入。`long_sequence()` 的普通和带随机种子工厂保证 `x` 从 1 递增，可以使用串行 stream。其他工厂默认不提供证明。输入还必须满足 `isStableWithinExecution()`。

### 2.2 键函数的保序能力

复用 [MonotonicTimestampFunction](../core/src/main/java/io/questdb/griffin/engine/functions/MonotonicTimestampFunction.java)，增加默认关闭的 `preservesTimestampOrder()`。区间反推接口的 `EXACT` / `SUPERSET` 是过滤裁剪能力，不能直接当成分组连续证明。

当前只证明直接作用于输入 timestamp 列的一层变换：

| 函数 | 启用条件 |
| --- | --- |
| 原 timestamp 列 | 物理正向有序 |
| `timestamp_floor`、`date_trunc` | 指定单位和 stride 的实际实现不发生回绕；日历月、年支持大于 1 的 stride |
| 带 origin 的 floor | 非负 origin，且对应单位的实现保序 |
| 固定时区的 floor / floor_utc | 有效 origin 为 0，精度一致，偏移和桶宽通过边界检查 |
| 命名时区的 `timestamp_floor_utc` | 返回 UTC、有效 origin 为 0、日内单位、标准偏移不导致回绕 |
| `timestamp_ceil` | 微秒精度；纳秒精度仅恒等的 `n` 单位 |
| 固定单位的常量 `dateadd`，或日历单位零位移 | 精度一致，实际常量位移在整个输入域内不回绕；四参数形式只接受固定时区规则 |
| timestamp 加减常量 long、固定偏移的 `to_timezone/to_utc` | 非 NULL 常量、精度一致、位移不回绕 |
| `year(ts)`、timestamp 转 long | 分别得到非递减 int 或相同 epoch 值 |
| timestamp_ns 转 timestamp | 降精度保持非递减；反向转换不在此范围 |

边界判断使用 timestamp driver 的输入域。微秒 designated timestamp 的上限是 9999 年末；纳秒域还要兼容历史表中的 `Long.MAX_VALUE` 附近数据。因此，纳秒正向位移、可能向上溢出的 ceil、内部先加三天的 week floor 等情况要拒绝。微秒时间戳上带 stride 的纳秒 floor 内部有乘 1000，不能只凭数学上的 floor 性质启用。

这里证明的是代码实际执行的函数，而不只是函数的数学名称。月份和年份桶没有固定 `bucketWidth`，仍可通过键比较查找连续区间，不依赖 PR 7706 的缓存区间接口。

### 2.3 DST 反例必须保留

命名时区的日桶带本地偏移时，UTC 输入递增不保证桶值递增。测试在纽约 2024 年回拨期间，以 `01:30` 为日桶边界，出现先到一个桶、进入另一个桶、又回到前一个桶的情况。正确 hash 结果为：

```text
bucket                       count  sum
2024-11-02T05:30:00.000000Z    3      8
2024-11-03T05:30:00.000000Z    2      7
2024-11-03T06:30:00.000000Z    2      13
```

这个查询明确回退到 hash。不能在运行时看到几个连续值就开始输出已完成的组：后续行可能重新落回旧组。

同样，若输入按 `(ts, symbol)` 排序，非严格单调的 `floor(ts)` 会合并多个 ts；这不意味着 `(floor(ts), symbol)` 仍有序。本实现通过只接收一个分组键避免这个错误推导。

## 3. 执行结构

### 3.1 串行

[StreamGroupByRecordCursorFactory](../core/src/main/java/io/questdb/griffin/engine/groupby/StreamGroupByRecordCursorFactory.java) 复用现有 `GroupByFunctionsUpdater`，维护一个 native `FlyweightPackedMapValue` 和一行 lookahead。

- 首行调用 `updateNew`；同键行调用 `updateExisting`。
- 发现新键后，返回当前组；下一次 `hasNext()` 从已经读到的新键开始。
- 不建立 map，不物化全部结果；状态空间是一个分组的状态大小。
- 输入遍历和聚合循环检查现有 circuit breaker。

串行并未新增 SIMD 接口，性能收益主要来自省去 hash/map 访问及结果物化。原本不支持并行的 `isOrdered`、`haversine_dist_deg` 和动态 symbol 参数仍可使用这个执行器。

### 3.2 并行 reducer

[AsyncStreamGroupByRecordCursorFactory](../core/src/main/java/io/questdb/griffin/engine/table/AsyncStreamGroupByRecordCursorFactory.java) 使用现有 `PageFrameSequence`，每个 frame 独立生成有序的部分聚合状态。

对 frame 内的一个组：

1. 计算起点键。
2. 利用已证明的连续性，倍增探测后做二分，定位第一个不同键。常量键或大桶无需逐行求 floor。
3. 在 task 的 native 缓冲中建立一个 packed 状态，先调用 `updateEmpty`。
4. 对具备 `computeBatch` 能力、参数为直接物理列、类型匹配的函数，调用现有批处理。`sum/avg` 等复用已有 `Vect` native kernel。
5. 表达式参数、不支持 batch 的函数，以及有 column top 或类型转换的 frame，调用已有 `computeFirst/computeNext`。

函数参数不要求全都能够 batch；一个查询内可以混用批量和标量聚合。批处理的 rowId 使用 frame 的原始行号，保证 `first/last` 和 `arg_min/arg_max` 的合并语义。

[AsyncStreamGroupByAtom](../core/src/main/java/io/questdb/griffin/engine/table/AsyncStreamGroupByAtom.java) 复用 `PerWorkerLocks`、worker 函数克隆、`PerWorkerFunctionList.init` 和现有关闭逻辑。独立可变函数实例不在 worker 间无保护共享。

### 3.3 按序收集与内存所有权

[AsyncStreamGroupByRecordCursor](../core/src/main/java/io/questdb/griffin/engine/table/AsyncStreamGroupByRecordCursor.java) 通过 `next(dispatchLimit, false)` 按输入 frame 顺序收集。当前组的状态由 collector 独立持有；相邻 frame 的首尾同键部分调用原有 `merge`。看见下一组后即可返回结果。

结果缓冲放在 [PageFrameReduceTask](../core/src/main/java/io/questdb/cairo/sql/async/PageFrameReduceTask.java)，与现有有序任务的结果生命周期一致，不在 atom 上新增槽环，也不修改共享调度协议。`groupByValues` 按需分配并绑定查询 `MemoryTracker`。整个 task 消费完后才 collect，随后释放 native 缓冲并解除 tracker 绑定。其他任务类型不为此分配缓冲。

设 frame 上限为 F 行、在途任务数量为 Q、一个 packed 状态为 S 字节：最坏部分状态空间约为 `O(Q × F × S)`，collector 另需 `O(S)`。它不随整个查询的分组数无限增长，但不能称为一个状态的“常数内存”。Q 受现有队列容量和 dispatch limit 约束；现有 frame 地址缓存另占空间。

异常、取消、提前关闭、内存限制和 `toTop()` 都必须先正确收集或等待已发布任务，再释放、重置状态。专项测试覆盖故障后的同一 factory 重用和 native 内存归零。

## 4. 聚合函数契约与支持范围

[GroupByFunction](../core/src/main/java/io/questdb/griffin/engine/functions/GroupByFunction.java) 的 `isStateSelfContained()` 默认 false，含义是：分组状态完整存在 MapValue 中，复制或合并后源状态可以立即重用或释放。它与 `supportsParallelism()`、`supportsBatchComputation()` 是三个独立条件。

`computeBatch` 的注释已修正：batch 之后可以 merge。已有并行 not-keyed 路径本来也依赖这个行为。不能因为函数支持 keyed batch，就推断它的无键 batch 状态一定能够随意复制和释放。

已审核并启用的家族：

| 家族 | 覆盖 |
| --- | --- |
| Count | `count(*)` 和继承通用 count 状态的各参数类型 |
| 数值基本聚合 | 已有定长实现的 sum、avg、min/max、ksum/nsum、geomean、sum(long256) |
| 首尾值 | 布尔、整数、浮点、字符、date/timestamp、symbol、IPv4、UUID、GeoHash；对应 first/last/not-null 版本 |
| Decimal | 各存储宽度的 sum、avg（包括 rescale）、min/max、first/last/not-null |
| 统计 | stddev/variance、covariance、correlation、regression、skewness、kurtosis、weighted stddev/avg、VWAP |
| 位运算和布尔 | byte/short/int/long 的 bit_and/or/xor，bool_and/or，mode(boolean) |
| 取关联值 | 已有定长返回值的 arg_min/arg_max，包括 UUID/timestamp 组合 |
| 只支持串行的定长状态 | isOrdered、haversine_dist_deg；仍尊重原有并行能力限制 |

Symbol 状态存的是 ID：静态 symbol table 的 ID 在 worker 间一致；动态 symbol 函数遵守其原有串行限制。两种新游标均按现有 GROUP BY 模式提供 symbol table 访问。

没有开启 distinct、近似 distinct、percentile、非布尔 mode、字符串值聚合、array 聚合、TWAP 等包含外部状态的实现。即使 MapValue 里看起来只有几个 long，这些 long 也可能是指针或某个函数实例中容器的索引。例如 distinct 的 merge 可直接接管源指针；直接复制后释放 frame，会留下无效地址。若以后扩展这些函数，需要明确的状态转移/复制和有界回收协议，不能只把 capability 改成 true。串行当前也使用同一份保守契约。

## 5. SQL、游标与 EXPLAIN

典型查询：

```sql
SELECT ts, sum(v)
FROM trades
SAMPLE BY 1m ALIGN TO CALENDAR;
```

并行计划：

```text
Async Stream Group By workers: 4
  keys: [ts]
  keyFunctions: [timestamp_floor_utc('1m',ts)]
  values: [sum(v)]
    PageFrame
        Row forward scan
        Frame forward scan on: trades
```

串行显示 `Stream Group By`。若输出包含有序 timestamp 键，正确设置 timestamp metadata，现有优化器可省去对应的排序。int/long 键不伪装成 timestamp；显式排序仍按现有规则执行。

新游标不提供随机访问，`size()` 为 -1，`toTop()` 重新执行稳定输入；已有测试相应更新为这些真实能力，而不是修改期望结果。共享物化 GROUP BY 和填充仍走原实现，避免破坏其 `recordAt`、重放和状态共享要求。非确定性输入或聚合参数不启用 stream。

并行 `prepareForDispatch()` 仍会枚举 frame。正向 LIMIT 有机会减少后续聚合工作，但不等于跳过全部准备工作；`ORDER BY ts DESC LIMIT N` 仍可能需要扫描和排序。本次没有反向并行 stream 或 Top-N 专用实现。

按序收集也存在队头阻塞：第一个慢 frame 会限制后续已完成 frame 的回收和新任务发布。Parquet 解码、frame 大小差异、昂贵表达式都可能放大这个成本。它是性能模型的一部分，不只是一个正确性问题。

## 6. 验证

[StreamGroupByTest](../core/src/test/java/io/questdb/test/griffin/StreamGroupByTest.java) 使用 5 组执行配置：串行；并行计划但仅查询线程执行；1 个 worker、每 frame 1 行；4 个 worker、每 frame 17 或 256 行。

主要 oracle 是同数据、同类型、没有 designated timestamp 的表，确保比较对象走 hash。每个用例还检查实际 factory/EXPLAIN，防止 stream 查询悄悄回退后得到虚假的覆盖。整数、Decimal、首尾值等逐值比较；浮点统计按规定小数位比较，允许不同并行合并顺序产生正常的舍入差异。

覆盖包括：

- 跨 frame 同组、每行一组、空表、单行、全 NULL 和仅一个非 NULL 的组。
- 所有上述函数家族；直接物理列的 batch 路径和表达式的标量路径。
- 微秒/纳秒、日历 stride、epoch/9999 年边界、固定时区、DST 切换、Parquet、column top。
- 列重排、查询 timestamp 覆盖普通乱序/NULL 列、过滤、LIMIT、聚合后过滤。
- 相同 factory 多次打开、部分消费后 `toTop()`、完整消费后 `toTop()`、提前关闭。
- bind variable 更新、静态和动态 symbol、CTE 重复引用、共享游标回归。
- 小队列和 dispatch limit、取消与恢复、内存超限后重用、tracker 内存归零和 worker lock 释放。
- 多键、DST 不保序、数值回绕、不支持的聚合和 FILL 的回退。

所有新增测试均通过公开 API 验证，没有使用反射。

本地相关回归：2,085 项，0 failure / 0 error，31 项由既有测试条件跳过；包括 `ParallelGroupByFuzzTest`、GROUP BY、SAMPLE BY（两种精度）、FILL、EXPLAIN、PageFrame 和共享游标测试。随后补充的完整 stream 专项为 170 项，全部通过；最后一轮边界与日期函数回归共 195 项，零失败。未声称运行整个仓库所有测试或多平台 CI。

## 7. JMH 对照方法与结果

基准：[SampleByStreamBenchmark](../benchmarks/src/main/java/org/questdb/SampleByStreamBenchmark.java)。每个 trial 创建独立表并编译一次查询，每个 invocation 打开游标、完整消费全部输出列并关闭。建表、编译和 Parquet 转换不计入聚合耗时；结果生成、必要排序和游标生命周期计入。

基线为上述 commit 的原执行器；两边使用同一份 benchmark class、相同 JVM 参数和数据。`expectedStrategy` 只检查计划，没有改变优化器行为。`keyed` 以及并行 `filtered` 是原路径的对照组。性能比较没有通过额外算术表达式、函数包装或人为不同的 SQL 强制 hash。

本机为 Apple M4 Pro / arm64，GraalVM JDK 25，JMH 1.37。每种配置 1 fork，3 × 1 秒预热、5 × 1 秒测量、1 个 JMH 调用线程；并行 worker 数另行配置，查询线程也可帮助执行。基准类默认更充分的 2 forks 和 5 次预热，便于正式复测。关闭 INFO 查询日志，两边均保留 ERROR 日志；测试与编译不与测量并行运行。

默认数据为 2,000,000 行，timestamp 间隔 1 毫秒；`v = x % 97`、`d = x % 31`。1h/1s/1T 桶分别产生 1 / 2,000 / 2,000,000 个时间组。默认 frame 为 100,000 行。

完整数据：[CSV](benchmarks/group-by-stream-aggregation.csv)，包含每项平均值、99.9% 置信区间半宽及各 fork 的原始测量样本。主矩阵 76 项；多键对照另有 4 项三 forks 复测。

### 1 秒桶：不同聚合

| 聚合 | 串行：hash → stream | 4 worker：hash → stream |
| --- | ---: | ---: |
| count + sum + avg | 24.164 → 15.732 ms（1.54×） | 6.619 → 0.423 ms（15.66×） |
| count(*) | 15.458 → 9.926 ms（1.56×） | 1.494 → 0.043 ms（34.60×） |
| double sum/avg/min/max | 44.387 → 18.148 ms（2.45×） | 7.969 → 1.760 ms（4.53×） |
| stddev_pop + corr + weighted_avg | 34.744 → 22.219 ms（1.56×） | 8.548 → 4.617 ms（1.85×） |
| Decimal sum/avg/min/max | 50.098 → 26.553 ms（1.89×） | 12.396 → 2.604 ms（4.76×） |
| long / symbol 的 first/last 与 long 的 not-null 版本 | 32.390 → 21.368 ms（1.52×） | 9.324 → 0.362 ms（25.74×） |
| sum(v+1)、avg(d*2)、sum(sqrt(d)) | 29.254 → 21.132 ms（1.38×） | 12.174 → 0.892 ms（13.64×） |
| v % 7 = 0 过滤 | 7.228 → 5.637 ms（1.28×） | 2.365 → 2.310 ms（1.02×） |

并行 filtered 的两边都使用 hash，表中 1.02× 是对照波动。其余列出的组合实际使用 stream；不能把 count-only 的最好倍数推广到统计函数。

### 桶密度、NULL 与 Parquet

| 场景，count + sum + avg | 串行：hash → stream | 4 worker：hash → stream |
| --- | ---: | ---: |
| native，1h，1 组 | 23.187 → 15.250 ms（1.52×） | 2.447 → 0.367 ms（6.66×） |
| native，1s，2,000 组 | 24.164 → 15.732 ms（1.54×） | 6.619 → 0.423 ms（15.66×） |
| native，1T，2,000,000 组 | 156.213 → 34.747 ms（4.50×） | 154.125 → 41.174 ms（3.74×） |
| native，1s，值列全 NULL | 27.972 → 13.738 ms（2.04×） | 6.500 → 0.421 ms（15.45×） |
| native，1s，每十行一个 NULL | 24.124 → 15.651 ms（1.54×） | 6.542 → 0.421 ms（15.53×） |
| Parquet，1h | 25.985 → 15.099 ms（1.72×） | 2.544 → 0.368 ms（6.92×） |
| Parquet，1s | 24.910 → 15.762 ms（1.58×） | 6.375 → 0.421 ms（15.13×） |
| Parquet，1T | 182.609 → 34.483 ms（5.30×） | 184.665 → 40.800 ms（4.53×） |
| native，1s，America/New_York | 21.242 → 14.471 ms（1.47×） | 5.595 → 0.413 ms（13.54×） |

**每行一组时，并行 stream 比串行 stream 慢**：native 为 41.174 ms 对 34.747 ms。大量部分状态的写入、复制、收集和输出会抵消并行收益；这次并未加入按基数切换串行/并行的成本模型。

Parquet 数据同样是规则时间间隔和低基数数值，压缩、解码比较有利；这里没有覆盖随机时间间隔和复杂压缩布局。

### frame 大小与 worker 数

| native，1s，count + sum + avg | hash | stream | 加速比 |
| --- | ---: | ---: | ---: |
| 4 worker，frame 1,000 行 | 6.612 ms | 0.925 ms | 7.15× |
| 4 worker，frame 100,000 行 | 6.619 ms | 0.423 ms | 15.66× |
| 4 worker，frame 1,000,000 行 | 14.295 ms | 0.945 ms | 15.13× |
| 1 worker，frame 100,000 行 | 13.287 ms | 0.960 ms | 13.85× |
| 8 worker，frame 100,000 行 | 5.065 ms | 0.305 ms | 16.59× |

frame 太小会增加任务、分配和收集次数；太大又减少可并行任务。默认 100,000 行在这组数据中较合适，不能推断 worker 越多或 frame 越大就越快。

### 多键对照复测

`(bucket, symbol)` 两边都走 hash。初次单 fork 串行是 47.237 → 60.455 ms；因此追加每边 3 forks、每 fork 5 次预热和 5 次测量：

| 执行 | 基线 hash | 本次代码的 hash |
| --- | ---: | ---: |
| 串行 | 53.251 ± 7.800 ms | 45.861 ± 0.443 ms |
| 4 worker | 18.238 ± 0.190 ms | 19.203 ± 1.474 ms |

不同 fork 出现快慢两档，初次退化未稳定复现；两边区间重叠。这个结果不能证明两者完全等速，也不能归因于 stream：当前查询根本没有使用 stream。保留原始样本，不把这些差异计入收益。

这些是本机短测，不能替代生产数据、不同 CPU、持续并发和 p95/p99 测量。尤其要分别观察串行/并行、少组/多组、batch/表达式，以及继续走 hash 的对照组；不以最好的一项作为总体加速比。

### 复现

构建 benchmark 模块后，按下面的参数运行。JVM 需保留项目针对 JDK 25 的 native access、Unsafe 和模块开放参数；`-jvmArgs` 也要传给 JMH fork。将 INFO 日志关闭的配置传入 `-Dout=/absolute/path/quiet-log.conf`。

```properties
writers=stdout
w.stdout.class=io.questdb.log.LogConsoleWriter
w.stdout.level=ERROR
```

```bash
mvn -pl benchmarks -am -DskipTests package
JMH_JVM_ARGS="-ea -Dfile.encoding=UTF-8 -XX:+UseParallelGC --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Dout=/absolute/path/quiet-log.conf"
java $JMH_JVM_ARGS -jar benchmarks/target/benchmarks.jar \
  SampleByStreamBenchmark -p expectedStrategy=stream \
  -p bucket=1s -p workers=0,4 \
  -p workload=basic,count,double,stats,decimal,first_last,expression,filtered,keyed \
  -wi 3 -w 1s -i 5 -r 1s -f 1 -t 1 \
  -jvmArgs "$JMH_JVM_ARGS" -rf json -rff stream.json
```

基线构建中复制同一份 benchmark 源文件，并改用 `expectedStrategy=hash`；也可以像本次测量一样，将同一份已编译的 JMH class 分别链接到两份 core classes。其余参数不变。

完整矩阵在上述 workload 组合外，分别运行：

| 维度 | 参数覆盖 |
| --- | --- |
| 桶密度 | basic，bucket=1h/1s/1T，workers=0/4 |
| NULL | basic，bucket=1s，nullEvery=1/10，workers=0/4 |
| Parquet | basic，bucket=1h/1s/1T，workers=0/4 |
| frame 大小 | basic，bucket=1s，frameRows=1000/100000/1000000，workers=4 |
| worker 数量 | basic，bucket=1s，workers=0/1/4/8 |
| 命名时区 | basic，bucket=1s，timeZone=America/New_York，workers=0/4 |

`nullEvery=0` 表示无 NULL，1 表示聚合值列全 NULL，10 表示每十行一个 NULL。`count(*)` 始终统计所有行。
