# AdaptiveBufferedThreadPoolExecutor

[![Thread Pool CI](https://github.com/huihuizhou00/AdaptiveBufferedThreadPoolExecutor/actions/workflows/ci.yml/badge.svg)](https://github.com/huihuizhou00/AdaptiveBufferedThreadPoolExecutor/actions/workflows/ci.yml)

一个面向**高并发线程池可靠性优化与自动化测试**的 Java 8 项目。

项目基于 JDK `ThreadPoolExecutor` 的调度模型，引入可配置的 **Buffer Degree** 提前扩容机制，并围绕线程池饱和处理、任务完整记账、超时关闭、中断传播、线程泄漏检测、结构化报告和持续集成建立了一套可重复验证的工程化体系

---
## 1. 项目背景

JDK 线程池默认的任务处理顺序为：

```text
创建核心线程
→ 核心线程达到上限
→ 任务进入阻塞队列
→ 队列满后创建非核心线程
→ 达到最大线程数后执行拒绝策略
```

这种策略倾向于优先使用队列。在突发 IO 负载下，非核心线程可能扩展较晚，导致任务快速堆积，并在短时间内触发拒绝。

本项目重点研究以下问题：

- 如何通过可配置水位提前创建非核心线程；
- 队列饱和后如何进行有限重试或提交侧反压；
- 如何完整统计成功、失败、拒绝和未决任务；
- 如何验证超时、中断、强制关闭和线程释放等异常路径；
- 如何将线程池实验接入 JUnit 和 GitHub Actions；
- 如何将控制台输出转换为可解析的 CSV、JSON 报告。

---

## 2. 核心机制

### 2.1 Buffer Degree 提前扩容

Buffer Degree 用于计算队列扩容水位：

```text
扩容水位 = 队列容量 × Buffer Degree
```

当队列任务数超过该水位时，线程池会更早尝试创建非核心线程。

| Buffer Degree | 行为倾向 |
|---:|---|
| `0.0` | 更早创建非核心线程 |
| `0.5` | 在中间队列水位扩容 |
| `1.0` | 更接近 JDK 的队列优先行为 |

需要注意：

- Buffer Degree 不会改变 `BlockingQueue` 的实际容量；
- Buffer Degree 不是运行时动态参数；
- Buffer Degree 越低，不代表性能一定越好；
- 更早扩容会增加线程资源消耗，需要结合业务负载评估。

在当前快速回归场景中，使用相同的线程数、队列容量和任务负载，稳定观察到：

```text
Buffer Degree 0.0 → peakPoolSize = 4
Buffer Degree 0.5 → peakPoolSize = 3
Buffer Degree 1.0 → peakPoolSize = 2
```

该结果验证了 Buffer Degree 改变的是**扩容时机**，而不是队列容量。

### 2.2 饱和兜底策略

项目实现并测试了三种饱和处理方式：

| 策略 | 机制 | 主要取舍 |
|---|---|---|
| BISS | 短暂退避后重试入队 | 返回较快，但达到重试上限后仍可能拒绝 |
| BWS | 限时阻塞等待队列空位 | 可以降低拒绝，但会阻塞提交线程 |
| RQS | 再执行一次非阻塞入队 | 返回快，但强饱和时容易拒绝 |

快速回归中的典型行为：

| 场景 | success | failed | rejected | 关键现象 |
|---|---:|---:|---:|---|
| JDK-SATURATION | 2 | 0 | 10 | 强饱和后直接拒绝 |
| ABTP-BISS | 2 | 0 | 10 | 命中短退避重试路径 |
| ABTP-BWS | 12 | 0 | 0 | 通过提交侧等待避免拒绝 |
| ABTP-RQS | 2 | 0 | 10 | 单次重试后仍可能拒绝 |

BWS 的零拒绝来自**提交侧反压**。在当前快速测试中，BWS 的提交耗时明显高于其他策略，因此不能将“零拒绝”直接解释为“吞吐量更高”或“延迟更低”。

### 2.3 可靠任务记账

每个实验统一统计以下状态：

```text
submitted
success
failed
rejected
unresolved
```

完整记账关系为：

```text
success + failed + rejected + unresolved = submitted
```

正常完成的回归实验要求：

```text
unresolved = 0
accounted = true
terminated = true
```

字段含义：

- `success`：任务成功执行完成；
- `failed`：任务已被线程池接收，但执行失败或被中断；
- `rejected`：任务在提交阶段未被线程池接收；
- `unresolved`：实验结束时仍未获得明确结果的任务；
- `accounted`：所有任务是否完成分类；
- `terminated`：线程池是否最终终止；
- `timedOut`：实验是否进入过超时强制清理流程。

### 2.4 超时与强制关闭

超时专项场景：

```text
1 个运行任务
+ 3 个排队任务
= 4 个已接收任务
```

每个任务执行 5 秒，实验等待时间为 1 秒。触发超时后：

```text
正常等待超时
→ shutdownNow()
→ 中断运行任务
→ 取出排队任务
→ 统一失败记账
→ 等待 Worker 退出
```

当前回归结果：

```text
strategy = ABTP-TIMEOUT
submitted = 4
success = 0
failed = 4
rejected = 0
unresolved = 0
accounted = true
terminated = true
timedOut = true
```

这说明线程池虽然没有在规定时间内自然结束，但经过强制清理后，所有任务都得到明确分类，线程池也最终正常退出。

### 2.5 中断传播与线程泄漏检测

项目针对提交线程在 `submitIntervalMs` 等待期间被中断的场景建立了自动化测试，验证：

- 停止继续提交后续任务；
- `InterruptedException` 能够向调用方传播；
- 清理完成后恢复线程中断标记；
- 调用 `shutdownNow()` 清理 Worker 和队列；
- 通过自定义 `ThreadFactory` 为 Worker 命名；
- 测试结束后不存在指定名称前缀的活动 Worker。

中断路径采用以下顺序：

```text
捕获 InterruptedException
→ shutdownNow()
→ 等待 Worker 响应中断并退出
→ 恢复当前线程中断标记
→ 重新抛出 InterruptedException
```

---

## 3. 项目结构

```text
.
├── .github/
│   └── workflows/
│       └── ci.yml
├── pom.xml
├── README.md
└── src/
    ├── main/java/pool/
    │   ├── AdaptiveBufferedThreadPoolExecutor.java
    │   ├── RejectedExecutionHandler.java
    │   ├── Test.java
    │   ├── ThreadPoolExperimentResult.java
    │   ├── ThreadPoolExperimentRunner.java
    │   ├── ThreadPoolExperimentReportWriter.java
    │   └── ThreadPoolReportMain.java
    └── test/java/pool/
        ├── BuildSmokeTest.java
        ├── ThreadPoolExperimentResultTest.java
        ├── ThreadPoolExperimentRunnerTest.java
        ├── ThreadPoolExperimentReportWriterTest.java
        └── ThreadPoolReportMainTest.java
```

主要类职责：

| 类 | 职责 |
|---|---|
| `AdaptiveBufferedThreadPoolExecutor` | 自定义线程池核心实现 |
| `RejectedExecutionHandler` | 项目内自定义拒绝处理器接口 |
| `Test` | 原始实验和长负载基准入口 |
| `ThreadPoolExperimentResult` | 保存结构化实验结果 |
| `ThreadPoolExperimentRunner` | 统一执行 JDK 与 ABTP 实验 |
| `ThreadPoolExperimentReportWriter` | 输出 CSV 和 JSON |
| `ThreadPoolReportMain` | 编排正式快速回归实验 |
| `ThreadPoolExperimentRunnerTest` | 负载、策略、超时、中断和线程释放测试 |
| `ThreadPoolExperimentReportWriterTest` | CSV、JSON 文件输出测试 |
| `ThreadPoolReportMainTest` | 正式报告编排和落盘验证 |

---

## 4. 自动化测试矩阵

当前测试覆盖：

| 测试场景 | 验证重点 |
|---|---|
| Maven 构建冒烟 | Java 8、JUnit 和 Surefire 配置正常 |
| JDK 低负载 | 容量足够时任务全部成功 |
| JDK 强饱和 | 达到线程和队列上限后产生拒绝 |
| 参数化非法输入 | 核心线程数、最大线程数、容量和时间参数校验 |
| ABTP-BISS | 短退避路径、拒绝与完整记账 |
| ABTP-BWS | 限时阻塞、提交侧反压和零拒绝 |
| ABTP-RQS | 单次非阻塞重试 |
| Buffer Degree 0.0 | 更早扩容 |
| Buffer Degree 0.5 | 中间扩容水位 |
| Buffer Degree 1.0 | 更晚扩容 |
| ABTP-TIMEOUT | 强制关闭和失败任务记账 |
| 提交线程中断 | 异常传播、中断标记恢复和 Worker 清理 |
| 结果对象测试 | 派生指标和完整性判断 |
| CSV 报告 | 表头、字段顺序、数据一致性 |
| JSON 报告 | JSON 格式、类型和字段完整性 |
| 正式报告编排 | 8 组实验完整执行并落盘 |

当前基线共执行 25 项 JUnit 测试，要求：

```text
Failures = 0
Errors = 0
Skipped = 0
```

---

## 5. 环境要求

```text
Java 8
Maven 3
Git
```

主要依赖：

```text
JUnit Jupiter 5.10.2
Maven Surefire Plugin 3.2.5
OSHI 6.4.10
SLF4J Simple 2.0.9
```

检查环境：

```bash
java -version
mvn -version
```

---

## 6. 快速运行

执行全部自动化测试：

```bash
mvn clean test
```

成功时应看到：

```text
BUILD SUCCESS
```

正式快速回归报告由 `ThreadPoolReportMainTest` 在测试过程中自动生成。

单独执行正式报告测试：

```bash
mvn test -Dtest=ThreadPoolReportMainTest
```

单独执行策略矩阵：

```bash
mvn test   -Dtest=ThreadPoolExperimentRunnerTest#shouldExerciseAbtpSaturationStrategies   -Dsurefire.useFile=false
```

单独执行 Buffer Degree 测试：

```bash
mvn test   -Dtest=ThreadPoolExperimentRunnerTest#shouldExpandEarlierWhenBufferDegreeIsLower   -Dsurefire.useFile=false
```

单独执行超时专项：

```bash
mvn test   -Dtest=ThreadPoolExperimentRunnerTest#shouldForceShutdownAndAccountEveryTaskAfterTimeout   -Dsurefire.useFile=false
```

---

## 7. 报告输出

执行：

```bash
mvn clean test
```

后会生成：

```text
target/thread-pool-report/results.csv
target/thread-pool-report/results.json
target/surefire-reports/
```

CSV 包含以下字段：

```text
strategy
submitted
success
failed
rejected
unresolved
forceEnqueueCount
peakPoolSize
submitMs
totalMs
accounted
terminated
timedOut
```

检查 CSV：

```bash
cat target/thread-pool-report/results.csv
```

检查 CSV 逻辑行数：

```bash
sed -n '$=' target/thread-pool-report/results.csv
```

预期为：

```text
9
```

即：

```text
1 行表头 + 8 行实验结果
```

检查 JSON 格式：

```bash
python3 -m json.tool   target/thread-pool-report/results.json
```

---

## 8. 正式快速回归场景

正式报告包含 8 组实验：

```text
JDK-SATURATION
ABTP-BISS
ABTP-BWS
ABTP-RQS
ABTP-BUFFER-0.0
ABTP-BUFFER-0.5
ABTP-BUFFER-1.0
ABTP-TIMEOUT
```

所有场景必须满足：

```text
accounted = true
terminated = true
unresolved = 0
```

只有 `ABTP-TIMEOUT` 允许：

```text
timedOut = true
```

快速回归用于验证功能正确性和生命周期，不作为正式性能结论。

---

## 9. 长负载实验

本地完成了 5 轮、每轮 10000 个模拟 IO 任务的对照实验。

平均结果：

| 策略 | success | rejected | throughput（task/s） | P99 提交耗时（ms） |
|---|---:|---:|---:|---:|
| BISS | 9968.0 | 32.0 | 1943.898 | 0.0754 |
| BWS | 10000.0 | 0.0 | 1936.416 | 0.0760 |
| RQS | 9940.8 | 59.2 | 1940.652 | 0.0518 |
| JDK | 9957.2 | 42.8 | 1942.244 | 0.0444 |

相对于 JDK：

- BISS 平均拒绝数下降约 `25.2%`；
- BISS 与 JDK 的完成吞吐量基本持平；
- BWS 在该固定负载下实现零拒绝；
- BWS 通过阻塞提交线程形成反压，吞吐量略低；
- RQS 在当前参数下没有表现出拒绝优势；
- JDK 的平均 P99 提交耗时更低；
- CPU 数据波动较大，不足以证明某种策略具有稳定 CPU 优势。

因此本项目不声明：

- ABTP 的吞吐量显著高于 JDK；
- ABTP 的 P99 提交延迟优于 JDK；
- ABTP 的 CPU 消耗稳定低于 JDK。

长负载实验不作为 GitHub 共享 Runner 中的 CI 硬门禁。

---

## 10. GitHub Actions

工作流文件：

```text
.github/workflows/ci.yml
```

触发条件：

```text
push
pull_request
```

CI 流程：

```text
Checkout
→ 配置 Java 8
→ Maven clean test
→ 校验 CSV 和 JSON
→ 校验任务完整记账
→ 上传测试和实验报告
```

工作流结束后，可以在 GitHub Actions 运行详情页底部下载：

```text
thread-pool-test-artifacts
```

其中包含：

```text
surefire-reports/
thread-pool-report/
├── results.csv
└── results.json
```

---



