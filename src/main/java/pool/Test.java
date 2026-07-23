package pool;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

public class Test {
    //新增字段
    private static final SystemInfo SYSTEM_INFO =
        new SystemInfo();

    private static final OperatingSystem OPERATING_SYSTEM =
            SYSTEM_INFO.getOperatingSystem();

    private static final CentralProcessor PROCESSOR =
            SYSTEM_INFO.getHardware().getProcessor();

    private static final java.lang.management.OperatingSystemMXBean
            BASE_OS_BEAN =
            ManagementFactory.getOperatingSystemMXBean();

    private static final com.sun.management.OperatingSystemMXBean
            JVM_OS_BEAN =
            BASE_OS_BEAN
                    instanceof com.sun.management.OperatingSystemMXBean
                    ? (com.sun.management.OperatingSystemMXBean)
                            BASE_OS_BEAN
                    : null;

    private static boolean isBuffer = true;
    private static void reliableTest2()
            throws InterruptedException {

        int corePoolSize = 16;
        int maximumPoolSize = 100;
        int queueSize = 300;
        double bufferDegree = 1.0;

        reliableABTPTest(
                "BISS",
                corePoolSize,
                maximumPoolSize,
                queueSize,
                bufferDegree,
                true,
                -1,
                1
        );

        reliableABTPTest(
                "BWS",
                corePoolSize,
                maximumPoolSize,
                queueSize,
                bufferDegree,
                true,
                100,
                1
        );

        reliableABTPTest(
                "RQS",
                corePoolSize,
                maximumPoolSize,
                queueSize,
                bufferDegree,
                true,
                -1,
                -1
        );

        reliableJDKTest(
                corePoolSize,
                maximumPoolSize,
                queueSize
        );
    }

    public static void main(String[] args)
        throws InterruptedException {

    // 预热轮，不纳入正式统计
    System.out.println("===== WARMUP =====");
    reliableTest2();

    Thread.sleep(2000);

    // 正式测试5轮
    for (int round = 1; round <= 5; round++) {
        System.out.println(
                "===== ROUND " + round + " ====="
        );

        reliableTest2();

        // 让上一轮线程、CPU采样及系统资源充分释放
        Thread.sleep(2000);
    }
}


    // public static void main(String[] args) throws InterruptedException {

        // test1(16, 100, 300, 0.2);
        //  test2(16, 100, 300, 1.0);

        //  AdaptiveBufferedThreadPoolExecutor.BasicCalculate basicCalculate = new AdaptiveBufferedThreadPoolExecutor.BasicCalculate();
        //  basicCalculate.updateCPULoad();
        //  System.out.println(basicCalculate.getCPULoad());
    // }

    // 缓冲扩展策略->(已执行任务数, 线程数, 执行时间, 拒绝策略执行次数)
    // 测试ABTP在不同阻塞度条件下的表现情况
    public static void test1(int corePoolSize, int maximumPoolSize, int queueSize, double bufferDegreeBase) throws InterruptedException {
        for (int i = 0; i <= 5; i++) {
            String bufferDegree = String.format("%.1f", bufferDegreeBase*i);
            System.out.print("ABTP-" + bufferDegree + ":");
            ABTP_Test(corePoolSize, maximumPoolSize, queueSize, bufferDegreeBase*i, false);
            System.out.println();
            Thread.sleep(1000);
        }
        System.out.print("JDKTP:");
        JDKTP_Test(corePoolSize, maximumPoolSize, queueSize);
        Thread.sleep(1000);
        System.out.print("\nTTP:");
        TTP_Test(corePoolSize, maximumPoolSize, queueSize);
    }

    // 强制入队模块测试->(已执行任务数, 线程数, 执行时间, 拒绝策略执行次数)
    // 测试ABTP的稳定性情况
    public static void test2(int corePoolSize, int maximumPoolSize, int queueSize, double bufferDegreeBase) throws InterruptedException {
        System.out.print("BISSJDKTP:");
        ABTP_Test(corePoolSize, maximumPoolSize, queueSize, bufferDegreeBase, true, -1, 1);
        System.out.println();
        System.out.print("BWSJDKTP:");
        ABTP_Test(corePoolSize, maximumPoolSize, queueSize, bufferDegreeBase, true, 100 ,1);
        System.out.println();
        System.out.print("RQSJDKTP:");
        ABTP_Test(corePoolSize, maximumPoolSize, queueSize, bufferDegreeBase, true, -1, -1);
        System.out.println();
        System.out.print("JDKTP:");
        JDKTP_Test(corePoolSize, maximumPoolSize, queueSize);
        System.out.println();
        System.out.print("TTP:");
        TTP_Test(corePoolSize, maximumPoolSize, queueSize);
    }

  

    private static void reliableABTPTest(
            String name,
            int corePoolSize,
            int maximumPoolSize,
            int queueSize,
            double bufferDegree,
            boolean preventRejection,
            int threadLoadJudge,
            double cpuLoadJudge)
            throws InterruptedException {

        final int batchCount = 50;
        final int tasksPerBatch = 200;
        final int totalTasks =
                batchCount * tasksPerBatch;

        ExperimentMetrics metrics =
                new ExperimentMetrics(totalTasks);

        AdaptiveBufferedThreadPoolExecutor threadPool =
                createABTP(
                        corePoolSize,
                        maximumPoolSize,
                        queueSize,
                        bufferDegree,
                        preventRejection,
                        threadLoadJudge,
                        cpuLoadJudge
                );

        threadPool.setRejectedExecutionHandler(
                new ABTPMetricsPolicy(metrics)
        );

        ResourceSnapshot resourceBefore = captureResourceSnapshot();

        long totalStart = System.nanoTime();
        long submitStart = System.nanoTime();
        

        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < tasksPerBatch; j++) {
                long executeStart = System.nanoTime();
                try {
                    threadPool.execute(
                            metrics.wrap(Test::IOTask)
                    );
                } finally {
                    metrics.recordExecuteLatency(
                            System.nanoTime() - executeStart
                    );
                }
            }

            if (i != batchCount - 1) {
                Thread.sleep(100);
            }
        }

        long submitEnd = System.nanoTime();

        threadPool.shutdown();

        boolean allAccounted =
                metrics.awaitAll(
                        30,
                        TimeUnit.SECONDS
                );

        boolean terminated =
                threadPool.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                );

        long totalEnd = System.nanoTime();
        ResourceSnapshot resourceAfter = captureResourceSnapshot();

        long submitMs =
                TimeUnit.NANOSECONDS.toMillis(
                        submitEnd - submitStart
                );

        long totalMs =
                TimeUnit.NANOSECONDS.toMillis(
                        totalEnd - totalStart
                );
        
        //完成吞吐量
        long totalNanos =
        totalEnd - totalStart;

        double totalSeconds =
                totalNanos / 1_000_000_000.0;

        int successCount =
                metrics.getSuccessCount();

        double completionThroughputTps =
        totalSeconds <= 0
                ? 0.0
                : successCount / totalSeconds;

        //进程CPU时间
        long processCpuNanos =
        resourceBefore.processCpuNanos < 0
        || resourceAfter.processCpuNanos < 0
                ? -1L
                : resourceAfter.processCpuNanos
                    - resourceBefore.processCpuNanos;

        double processCpuMs =
                processCpuNanos < 0
                        ? -1.0
                        : processCpuNanos / 1_000_000.0;

        //归一化进程CPU占用
        int processors =
        Runtime.getRuntime()
                .availableProcessors();

        double normalizedProcessCpuPct =
                processCpuNanos < 0
                || totalNanos <= 0
                        ? -1.0
                        : processCpuNanos
                            / (double) totalNanos
                            / processors
                            * 100.0;

        //每成功任务CPU成本
        double cpuMicrosPerSuccess =
            processCpuNanos < 0
            || successCount == 0
                    ? -1.0
                    : processCpuNanos
                        / 1_000.0
                        / successCount;

        //上下文切换增量
        long contextSwitches =
        resourceBefore.contextSwitches < 0
        || resourceAfter.contextSwitches < 0
                ? -1L
                : resourceAfter.contextSwitches
                    - resourceBefore.contextSwitches;

        double contextSwitchesPerSuccess =
                contextSwitches < 0
                || successCount == 0
                        ? -1.0
                        : contextSwitches
                            / (double) successCount;
        //整机CPU负载
        double systemCpuPct =
            PROCESSOR.getSystemCpuLoadBetweenTicks(
                    resourceBefore.systemCpuTicks
            ) * 100.0;
    
        System.out.println(
                name
                + ": submitted=" + totalTasks
                + ", success=" + metrics.getSuccessCount()
                + ", failed=" + metrics.getFailedCount()
                + ", rejected=" + metrics.getRejectedCount()
                + ", unresolved=" + metrics.getUnresolvedCount()
                + ", force=" + threadPool.getForceEnqueueCount()
                + ", submitMs=" + submitMs
                + ", totalMs=" + totalMs
                + ", accounted=" + allAccounted
                + ", terminated=" + terminated
                + ", executeP95Ms="
                + String.format(
                        "%.3f",
                        metrics.getExecuteP95Ms()
                )

                + ", executeP99Ms="
                + String.format(
                        "%.3f",
                        metrics.getExecuteP99Ms()
                )

                + ", throughputTps="
                + String.format(
                        "%.2f",
                        completionThroughputTps
                )

                + ", processCpuMs="
                + String.format(
                        "%.2f",
                        processCpuMs
                )

                + ", processCpuPct="
                + String.format(
                        "%.2f",
                        normalizedProcessCpuPct
                )

                + ", cpuUsPerSuccess="
                + String.format(
                        "%.3f",
                        cpuMicrosPerSuccess
                )

                + ", contextSwitches="
                + contextSwitches

                + ", ctxPerSuccess="
                + String.format(
                        "%.4f",
                        contextSwitchesPerSuccess
                )

                + ", systemCpuPct="
                + String.format(
                        "%.2f",
                        systemCpuPct
                )
        );

        if (!terminated) {
            threadPool.shutdownNow();
        }
    }

    public static void ABTP_Test(
        int corePoolSize,
        int maximumPoolSize,
        int queueSize,
        double bufferDegree,
        boolean preventRejection)
        throws InterruptedException {

    ABTP_Test(
            corePoolSize,
            maximumPoolSize,
            queueSize,
            bufferDegree,
            preventRejection,
            0,
            0.5
        );
    }
    //资源快照
    private static final class ResourceSnapshot {

        private final long processCpuNanos;
        private final long contextSwitches;
        private final long[] systemCpuTicks;

        private ResourceSnapshot(
                long processCpuNanos,
                long contextSwitches,
                long[] systemCpuTicks) {

            this.processCpuNanos = processCpuNanos;
            this.contextSwitches = contextSwitches;
            this.systemCpuTicks = systemCpuTicks;
        }
    }

    private static ResourceSnapshot captureResourceSnapshot() {
        long processCpuNanos =
                JVM_OS_BEAN == null
                        ? -1L
                        : JVM_OS_BEAN.getProcessCpuTime();

        OSProcess process =
                OPERATING_SYSTEM.getProcess(
                        OPERATING_SYSTEM.getProcessId()
                );

        long contextSwitches =
                process == null
                        ? -1L
                        : process.getContextSwitches();

        long[] systemCpuTicks =
                PROCESSOR.getSystemCpuLoadTicks();

        return new ResourceSnapshot(
                processCpuNanos,
                contextSwitches,
                systemCpuTicks
        );
    }
    
    public static void ABTP_Test(int corePoolSize, int maximumPoolSize, int queueSize, double bufferDegree, boolean isPreventRejection, Integer threadLoadJudge, double cpuLoadJudge) throws InterruptedException {
        AdaptiveBufferedThreadPoolExecutor threadPool = createABTP(corePoolSize, maximumPoolSize, queueSize, bufferDegree, isPreventRejection, threadLoadJudge, cpuLoadJudge);
        AdaptiveBufferedThreadPoolExecutor.CountPolicy countPolicy = (AdaptiveBufferedThreadPoolExecutor.CountPolicy) threadPool.getRejectedExecutionHandler();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 200; j++) {
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        IOTask();
                    }
                });
            }
            System.out.print("(" + (i+1)*200 + "," + threadPool.getPoolSize() + "," + (System.currentTimeMillis() - start) + "," + countPolicy.getCount() +  ","
    + threadPool.getForceEnqueueCount()+ ")");
            if (i != 9) {
                System.out.print("->");
            }
            if (isBuffer) {
                Thread.sleep(100);
            }
        }
        threadPool.shutdown();
    }

    

    private static void reliableJDKTest(
            int corePoolSize,
            int maximumPoolSize,
            int queueSize)
            throws InterruptedException {

        final int batchCount = 50;
        final int tasksPerBatch = 200;
        final int totalTasks =
                batchCount * tasksPerBatch;

        ExperimentMetrics metrics =
                new ExperimentMetrics(totalTasks);

        ThreadPoolExecutor threadPool =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        100,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(queueSize),
                        new JDKMetricsPolicy(metrics)
                );
        ResourceSnapshot resourceBefore =
                captureResourceSnapshot();
        long totalStart = System.nanoTime();
        long submitStart = System.nanoTime();

        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < tasksPerBatch; j++) {
                long executeStart = System.nanoTime();
                try {
                    threadPool.execute(
                            metrics.wrap(Test::IOTask)
                    );
                } finally {
                    metrics.recordExecuteLatency(
                            System.nanoTime() - executeStart
                    );
                }
            }

            if (i != batchCount - 1) {
                Thread.sleep(100);
            }
        }

        long submitEnd = System.nanoTime();

        threadPool.shutdown();

        boolean allAccounted =
                metrics.awaitAll(
                        60,
                        TimeUnit.SECONDS
                );

        boolean terminated =
                threadPool.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                );

        long totalEnd = System.nanoTime();
        ResourceSnapshot resourceAfter =
                captureResourceSnapshot();
        long submitMs =
                TimeUnit.NANOSECONDS.toMillis(
                        submitEnd - submitStart
                );

        long totalMs =
                TimeUnit.NANOSECONDS.toMillis(
                        totalEnd - totalStart
                );
        //完成吞吐量
        long totalNanos =
        totalEnd - totalStart;

        double totalSeconds =
                totalNanos / 1_000_000_000.0;

        int successCount =
                metrics.getSuccessCount();

        double completionThroughputTps =
        totalSeconds <= 0
                ? 0.0
                : successCount / totalSeconds;

        //进程CPU时间
        long processCpuNanos =
        resourceBefore.processCpuNanos < 0
        || resourceAfter.processCpuNanos < 0
                ? -1L
                : resourceAfter.processCpuNanos
                    - resourceBefore.processCpuNanos;

        double processCpuMs =
                processCpuNanos < 0
                        ? -1.0
                        : processCpuNanos / 1_000_000.0;

        //归一化进程CPU占用
        int processors =
        Runtime.getRuntime()
                .availableProcessors();

        double normalizedProcessCpuPct =
                processCpuNanos < 0
                || totalNanos <= 0
                        ? -1.0
                        : processCpuNanos
                            / (double) totalNanos
                            / processors
                            * 100.0;

        //每成功任务CPU成本
        double cpuMicrosPerSuccess =
            processCpuNanos < 0
            || successCount == 0
                    ? -1.0
                    : processCpuNanos
                        / 1_000.0
                        / successCount;

        //上下文切换增量
        long contextSwitches =
        resourceBefore.contextSwitches < 0
        || resourceAfter.contextSwitches < 0
                ? -1L
                : resourceAfter.contextSwitches
                    - resourceBefore.contextSwitches;

        double contextSwitchesPerSuccess =
                contextSwitches < 0
                || successCount == 0
                        ? -1.0
                        : contextSwitches
                            / (double) successCount;
        //整机CPU负载
        double systemCpuPct =
            PROCESSOR.getSystemCpuLoadBetweenTicks(
                    resourceBefore.systemCpuTicks
            ) * 100.0;
    
    
        System.out.println(
                "JDK"
                + ": submitted=" + totalTasks
                + ", success=" + metrics.getSuccessCount()
                + ", failed=" + metrics.getFailedCount()
                + ", rejected=" + metrics.getRejectedCount()
                + ", unresolved=" + metrics.getUnresolvedCount()
                + ", force=0"
                + ", submitMs=" + submitMs
                + ", totalMs=" + totalMs
                + ", accounted=" + allAccounted
                + ", terminated=" + terminated
                 + ", executeP95Ms="
                + String.format(
                        "%.3f",
                        metrics.getExecuteP95Ms()
                )

                + ", executeP99Ms="
                + String.format(
                        "%.3f",
                        metrics.getExecuteP99Ms()
                )

                + ", throughputTps="
                + String.format(
                        "%.2f",
                        completionThroughputTps
                )

                + ", processCpuMs="
                + String.format(
                        "%.2f",
                        processCpuMs
                )

                + ", processCpuPct="
                + String.format(
                        "%.2f",
                        normalizedProcessCpuPct
                )

                + ", cpuUsPerSuccess="
                + String.format(
                        "%.3f",
                        cpuMicrosPerSuccess
                )

                + ", contextSwitches="
                + contextSwitches

                + ", ctxPerSuccess="
                + String.format(
                        "%.4f",
                        contextSwitchesPerSuccess
                )

                + ", systemCpuPct="
                + String.format(
                        "%.2f",
                        systemCpuPct
                )
        );

        if (!terminated) {
            threadPool.shutdownNow();
        }
    }

    public static void JDKTP_Test(int corePoolSize, int maximumPoolSize, int queueSize) {
        CountPolicy countPolicy = new CountPolicy();
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            100,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize),
            countPolicy
        );
        
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 200; j++) {
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        IOTask();
                    }
                });
            }
            System.out.print("(" + (i+1)*200 + "," + threadPool.getPoolSize() + "," + (System.currentTimeMillis() - start) + "," + countPolicy.getCount()  + "0" + ")");

            if (i != 9) {
                System.out.print("->");
            }
            if (isBuffer) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        threadPool.shutdown();
    }

    public static void TTP_Test(int corePoolSize, int maximumPoolSize, int queueSize) throws InterruptedException {
        AdaptiveBufferedThreadPoolExecutor TTP = createTomcatThreadPool(corePoolSize, maximumPoolSize, queueSize);
        AdaptiveBufferedThreadPoolExecutor.CountPolicy countPolicy = (AdaptiveBufferedThreadPoolExecutor.CountPolicy) TTP.getRejectedExecutionHandler();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 200; j++) {
                TTP.execute(new Runnable() {
                    @Override
                    public void run() {
                        IOTask();
                    }
                });
            }
            System.out.print("(" + (i+1)*200 + "," + TTP.getPoolSize() + "," + (System.currentTimeMillis() - start) + "," + countPolicy.getCount() + "0" + ")");
            if (i != 9) {
                System.out.print("->");
            }
            if (isBuffer) {
                Thread.sleep(100);
            }
        }
        TTP.shutdown();
    }

    private static final class ExperimentMetrics {
        //新增指标
        private final long[] executeLatencyNanos;
        private final AtomicInteger executeLatencyIndex = new AtomicInteger(0);

        private final int totalTasks;
        private final CountDownLatch accountedLatch;

        private final AtomicInteger successCount =
                new AtomicInteger(0);

        private final AtomicInteger failedCount =
                new AtomicInteger(0);

        private final AtomicInteger rejectedCount =
                new AtomicInteger(0);

        private ExperimentMetrics(int totalTasks) {
            this.totalTasks = totalTasks;
            this.accountedLatch =
                    new CountDownLatch(totalTasks);
            this.executeLatencyNanos = new long[totalTasks];
        }

        private Runnable wrap(Runnable task) {
            return () -> {
                try {
                    task.run();
                    successCount.incrementAndGet();
                } catch (RuntimeException | Error e) {
                    failedCount.incrementAndGet();
                    throw e;
                } finally {
                    accountedLatch.countDown();
                }
            };
        }

        private void recordRejected() {
            rejectedCount.incrementAndGet();
            accountedLatch.countDown();
        }

        private boolean awaitAll(
                long timeout,
                TimeUnit unit)
                throws InterruptedException {

            return accountedLatch.await(timeout, unit);
        }

        private int getSuccessCount() {
            return successCount.get();
        }

        private int getFailedCount() {
            return failedCount.get();
        }

        private int getRejectedCount() {
            return rejectedCount.get();
        }

        private int getUnresolvedCount() {
            return totalTasks
                    - successCount.get()
                    - failedCount.get()
                    - rejectedCount.get();
        }
        //新增记录方法、分位数计算
        private void recordExecuteLatency(long latencyNanos) {
            int index = executeLatencyIndex.getAndIncrement();

            if (index < executeLatencyNanos.length) {
                executeLatencyNanos[index] = latencyNanos;
            }
        }
        private double getExecutePercentileMs(
        double percentile) {

            int size = Math.min(
                    executeLatencyIndex.get(),
                    executeLatencyNanos.length
            );

            if (size == 0) {
                return 0.0;
            }

            long[] copy = Arrays.copyOf(
                    executeLatencyNanos,
                    size
            );

            Arrays.sort(copy);

            int index = (int) Math.ceil(
                    percentile * size
            ) - 1;

            index = Math.max(
                    0,
                    Math.min(index, size - 1)
            );

            return copy[index] / 1_000_000.0;
        }

        private double getExecuteP95Ms() {
            return getExecutePercentileMs(0.95);
        }

        private double getExecuteP99Ms() {
            return getExecutePercentileMs(0.99);
        }
    }


    private static final class ABTPMetricsPolicy
                implements pool.RejectedExecutionHandler {

            private final ExperimentMetrics metrics;

            private ABTPMetricsPolicy(
                    ExperimentMetrics metrics) {
                this.metrics = metrics;
            }

            @Override
            public void rejectedExecution(
                    Runnable task,
                    AdaptiveBufferedThreadPoolExecutor executor) {

                metrics.recordRejected();
            }
        }

        private static final class JDKMetricsPolicy
            implements java.util.concurrent.RejectedExecutionHandler {

        private final ExperimentMetrics metrics;

        private JDKMetricsPolicy(
                ExperimentMetrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public void rejectedExecution(
                Runnable task,
                ThreadPoolExecutor executor) {

            metrics.recordRejected();
        }
    }

    public static AdaptiveBufferedThreadPoolExecutor createABTP(int corePoolSize, int maximumPoolSize, int queueSize, double bufferDegree, boolean isPreventRejection) {
        return new AdaptiveBufferedThreadPoolExecutor(corePoolSize, maximumPoolSize, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);
                    private final String namePrefix = "custom-thread-pool-";

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName(namePrefix + threadNumber.getAndIncrement());
                        t.setDaemon(false); // 非守护线程
                        t.setPriority(Thread.NORM_PRIORITY);
                        return t;
                    }
                }, new AdaptiveBufferedThreadPoolExecutor.CountPolicy(), bufferDegree, isPreventRejection, 5, 0.5, 10, 100, 3);
    }

    public static AdaptiveBufferedThreadPoolExecutor createABTP(int corePoolSize, int maximumPoolSize, int queueSize, double bufferDegree, boolean isPreventRejection, Integer threadLoadJudge, double cpuLoadJudge) {
        return new AdaptiveBufferedThreadPoolExecutor(corePoolSize, maximumPoolSize, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);
                    private final String namePrefix = "custom-thread-pool-";

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName(namePrefix + threadNumber.getAndIncrement());
                        t.setDaemon(false); // 非守护线程
                        t.setPriority(Thread.NORM_PRIORITY);
                        return t;
                    }
                }, new AdaptiveBufferedThreadPoolExecutor.CountPolicy(), bufferDegree, isPreventRejection, threadLoadJudge, cpuLoadJudge, 10, 100, 3);
    }

    public static AdaptiveBufferedThreadPoolExecutor createTomcatThreadPool(int corePoolSize, int maximumPoolSize, int queueSize) {
        return new AdaptiveBufferedThreadPoolExecutor(corePoolSize, maximumPoolSize, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);
                    private final String namePrefix = "custom-thread-pool-";

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName(namePrefix + threadNumber.getAndIncrement());
                        t.setDaemon(false); // 非守护线程
                        t.setPriority(Thread.NORM_PRIORITY);
                        return t;
                    }
                }, new AdaptiveBufferedThreadPoolExecutor.CountPolicy(), 0);
    }

    public static void IOTask(){
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class CountPolicy implements RejectedExecutionHandler {
        private static AtomicInteger count = new AtomicInteger(0);

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            count.incrementAndGet();
        }

        public int getCount() {
            return count.get();
        }
    }

    //新增指标

}
