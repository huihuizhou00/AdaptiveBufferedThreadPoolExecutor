package pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
class ThreadPoolExperimentRunnerTest {


    /**
     * 在指定时间内等待线程进入目标状态。
     */
    private static void waitUntilThreadReachesState(
            Thread thread,
            Thread.State expectedState,
            long timeoutMs)
            throws InterruptedException {

        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(
                                timeoutMs
                        );

        while (System.nanoTime() < deadline) {
            if (thread.getState() == expectedState) {
                return;
            }

            if (!thread.isAlive()) {
                fail(
                        "线程在进入目标状态前已经结束，"
                                + "当前状态：" + thread.getState()
                );
            }

            Thread.sleep(10L);
        }

        fail(
                "在" + timeoutMs
                        + "ms内未观察到线程进入状态："
                        + expectedState
                        + "，当前状态："
                        + thread.getState()
        );
    }

    /**
     * 等待所有指定名称前缀的线程退出。
     */
    private static void waitUntilNoLiveThreadWithPrefix(
            String threadNamePrefix,
            long timeoutMs)
            throws InterruptedException {

        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(
                                timeoutMs
                        );

        while (System.nanoTime() < deadline) {
            if (!hasLiveThreadWithPrefix(
                    threadNamePrefix
            )) {
                return;
            }

            Thread.sleep(20L);
        }

        Set<Thread> threads =
                Thread.getAllStackTraces().keySet();

        StringBuilder leakedThreadNames =
                new StringBuilder();

        for (Thread thread : threads) {
            if (thread.isAlive()
                    && thread.getName()
                        .startsWith(threadNamePrefix)) {

                if (leakedThreadNames.length() > 0) {
                    leakedThreadNames.append(", ");
                }

                leakedThreadNames.append(
                        thread.getName()
                );
            }
        }

        fail(
                "检测到未退出的ABTP Worker："
                        + leakedThreadNames
        );
    }   

    /**
     * JVM中是否仍存在指定名称前缀的活动线程。
     */
    private static boolean hasLiveThreadWithPrefix(
            String threadNamePrefix) {

        Set<Thread> threads =
                Thread.getAllStackTraces().keySet();

        for (Thread thread : threads) {
            if (thread.isAlive()
                    && thread.getName()
                        .startsWith(threadNamePrefix)) {
                return true;
            }
        }

        return false;
    }
        /**
     * ABTP饱和策略测试矩阵。
     */
    static Stream<Arguments> abtpStrategyScenarios() {
        return Stream.of(
                Arguments.of(
                        "ABTP-BISS",
                        -1,
                        1.0,
                        10L,
                        1000L,
                        3,
                        false
                ),
                Arguments.of(
                        "ABTP-BWS",
                        100,
                        1.0,
                        10L,
                        1000L,
                        3,
                        true
                ),
                Arguments.of(
                        "ABTP-RQS",
                        -1,
                        -1.0,
                        10L,
                        1000L,
                        3,
                        false
                )
        );
    }

    /**
     * 使用同一套实验执行器验证不同任务到达速率。
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("loadScenarios")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void shouldAccountTasksAcrossLoadScenarios(
            String strategyName,
            int corePoolSize,
            int maximumPoolSize,
            int queueCapacity,
            int taskCount,
            long taskDurationMs,
            long submitIntervalMs,
            boolean expectAllSuccess)
            throws InterruptedException {

        ThreadPoolExperimentResult result =
                ThreadPoolExperimentRunner.runJdkExperiment(
                        strategyName,
                        corePoolSize,
                        maximumPoolSize,
                        queueCapacity,
                        taskCount,
                        taskDurationMs,
                        submitIntervalMs,
                        5
                );

        /*
         * 所有场景都必须满足的正确性要求。
         */
        assertEquals(
                taskCount,
                result.getSubmitted()
        );

        assertEquals(
                taskCount,
                result.getResolvedTaskCount()
        );

        assertEquals(
                0,
                result.getFailed()
        );

        assertEquals(
                0,
                result.getUnresolved()
        );

        assertEquals(
                0,
                result.getForceEnqueueCount()
        );

        assertTrue(
                result.isAccountingComplete(),
                "所有任务必须被完整记账"
        );

        assertTrue(
                result.isTerminated(),
                "线程池必须正常终止"
        );

        assertTrue(
                result.isRunComplete(),
                "实验必须完整结束"
        );

        /*
         * 根据负载场景验证不同的业务结果。
         */
        if (expectAllSuccess) {
            assertEquals(
                    taskCount,
                    result.getSuccess()
            );

            assertEquals(
                    0,
                    result.getRejected()
            );

            assertEquals(
                    1.0,
                    result.getCompletionRate(),
                    0.0001
            );
        } else {
            assertTrue(
                    result.getSuccess() > 0,
                    "饱和场景下至少应有任务成功执行"
            );

            assertTrue(
                    result.getRejected() > 0,
                    "饱和场景下应触发任务拒绝"
            );

            assertEquals(
                    taskCount,
                    result.getSuccess()
                            + result.getRejected()
            );

            assertTrue(
                    result.getCompletionRate() < 1.0
            );
        }
    }

    /**
     * 负载测试矩阵。
     */
    static Stream<Arguments> loadScenarios() {
        return Stream.of(
                Arguments.of(
                        "JDK-SATURATION",
                        1,
                        1,
                        1,
                        20,
                        50L,
                        0L,
                        false
                ),
                Arguments.of(
                        "JDK-LOW-LOAD",
                        1,
                        1,
                        1,
                        5,
                        10L,
                        50L,
                        true
                )
        );
    }

    /**
     * 验证非法配置能够快速失败，而不是在线程池运行过程中
     * 才出现难以定位的问题。
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(
            String caseName,
            String strategyName,
            int corePoolSize,
            int maximumPoolSize,
            int queueCapacity,
            int taskCount,
            long taskDurationMs,
            long submitIntervalMs,
            long awaitTimeoutSeconds) {

        assertThrows(
                IllegalArgumentException.class,
                () -> ThreadPoolExperimentRunner
                        .runJdkExperiment(
                                strategyName,
                                corePoolSize,
                                maximumPoolSize,
                                queueCapacity,
                                taskCount,
                                taskDurationMs,
                                submitIntervalMs,
                                awaitTimeoutSeconds
                        ),
                caseName
        );
    }

    /**
     * 参数边界测试矩阵。
     */
    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                Arguments.of(
                        "策略名称为空",
                        " ",
                        1,
                        1,
                        1,
                        10,
                        10L,
                        0L,
                        5L
                ),
                Arguments.of(
                        "核心线程数为零",
                        "INVALID-CORE",
                        0,
                        1,
                        1,
                        10,
                        10L,
                        0L,
                        5L
                ),
                Arguments.of(
                        "最大线程数小于核心线程数",
                        "INVALID-MAX",
                        2,
                        1,
                        1,
                        10,
                        10L,
                        0L,
                        5L
                ),
                Arguments.of(
                        "队列容量为零",
                        "INVALID-QUEUE",
                        1,
                        1,
                        0,
                        10,
                        10L,
                        0L,
                        5L
                ),
                Arguments.of(
                        "任务数量为零",
                        "INVALID-TASK-COUNT",
                        1,
                        1,
                        1,
                        0,
                        10L,
                        0L,
                        5L
                ),
                Arguments.of(
                        "任务执行时间为负数",
                        "INVALID-DURATION",
                        1,
                        1,
                        1,
                        10,
                        -1L,
                        0L,
                        5L
                ),
                Arguments.of(
                        "提交间隔为负数",
                        "INVALID-INTERVAL",
                        1,
                        1,
                        1,
                        10,
                        10L,
                        -1L,
                        5L
                ),
                Arguments.of(
                        "等待超时时间为零",
                        "INVALID-TIMEOUT",
                        1,
                        1,
                        1,
                        10,
                        10L,
                        0L,
                        0L
                )
        );
    }

        /**
     * 验证三种ABTP饱和兜底策略的任务记账、
     * 拒绝行为和线程池生命周期。
     *
     * 这里通过阈值人为固定策略分支，
     * 不表示线程池能够自动选择最优策略。
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("abtpStrategyScenarios")
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void shouldExerciseAbtpSaturationStrategies(
            String strategyName,
            int threadLoadJudge,
            double cpuLoadJudge,
            long waitTime,
            long timeout,
            int maxRetryAttempts,
            boolean expectZeroReject)
            throws InterruptedException {

        int taskCount = 12;

        ThreadPoolExperimentResult result =
                ThreadPoolExperimentRunner.runAbtpExperiment(
                        strategyName,
                        1,
                        1,
                        1,
                        taskCount,
                        80L,
                        0L,
                        1.0,
                        true,
                        threadLoadJudge,
                        cpuLoadJudge,
                        waitTime,
                        timeout,
                        maxRetryAttempts,
                        10L
                );

        /*
        * 当前先保留控制台输出，方便人工观察。
        * 后续会统一替换为CSV和JSON报告。
        */
        System.out.println(
                "[ABTP-STRATEGY] " + result
        );

        /*
        * 所有策略都必须满足的正确性门禁。
        */
        assertEquals(
                taskCount,
                result.getSubmitted()
        );

        assertEquals(
                taskCount,
                result.getResolvedTaskCount()
        );

        assertEquals(
                0,
                result.getFailed()
        );

        assertEquals(
                0,
                result.getUnresolved()
        );

        assertTrue(
                result.getSuccess() > 0,
                "至少应有任务成功执行"
        );

        assertTrue(
                result.getForceEnqueueCount() > 0,
                "饱和场景下必须进入forceEnqueue流程"
        );

        assertTrue(
                result.isAccountingComplete(),
                "所有任务必须被完整记账"
        );

        assertTrue(
                result.isTerminated(),
                "线程池必须正常终止"
        );

        assertTrue(
                result.isRunComplete(),
                "实验必须完整结束"
        );

        /*
        * 不同策略的结果边界。
        */
        if (expectZeroReject) {
            assertEquals(
                    taskCount,
                    result.getSuccess(),
                    "BWS应在当前可控负载下完成全部任务"
            );

            assertEquals(
                    0,
                    result.getRejected(),
                    "BWS应通过限时阻塞避免当前负载下的拒绝"
            );
        } else {
            assertTrue(
                    result.getRejected() > 0,
                    strategyName
                            + "在当前强饱和负载下应出现拒绝"
            );

            assertEquals(
                    taskCount,
                    result.getSuccess()
                            + result.getRejected()
            );
        }
    }
        private ThreadPoolExperimentResult runBufferDegreeScenario(
            String strategyName,
            double bufferDegree)
            throws InterruptedException {

        return ThreadPoolExperimentRunner.runAbtpExperiment(
                strategyName,
                1,
                4,
                4,
                6,
                300L,
                0L,
                bufferDegree,
                false,
                -1,
                -1.0,
                10L,
                100L,
                3,
                5L
        );
    }
    private void assertCompleteWithoutRejection(
            ThreadPoolExperimentResult result,
            int expectedTaskCount) {

        assertEquals(
                expectedTaskCount,
                result.getSubmitted()
        );

        assertEquals(
                expectedTaskCount,
                result.getSuccess()
        );

        assertEquals(
                0,
                result.getFailed()
        );

        assertEquals(
                0,
                result.getRejected()
        );

        assertEquals(
                0,
                result.getUnresolved()
        );

        assertEquals(
                expectedTaskCount,
                result.getResolvedTaskCount()
        );

        assertTrue(
                result.isAccountingComplete()
        );

        assertTrue(
                result.isTerminated()
        );

        assertTrue(
                result.isRunComplete()
        );
    }

        /**
     * 验证Buffer Degree改变的是线程扩容水位，
     * 而不是BlockingQueue的实际容量。
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void shouldExpandEarlierWhenBufferDegreeIsLower()
            throws InterruptedException {

        ThreadPoolExperimentResult earlyExpansion =
                runBufferDegreeScenario(
                        "ABTP-BUFFER-0.0",
                        0.0
                );

        ThreadPoolExperimentResult middleExpansion =
                runBufferDegreeScenario(
                        "ABTP-BUFFER-0.5",
                        0.5
                );

        ThreadPoolExperimentResult lateExpansion =
                runBufferDegreeScenario(
                        "ABTP-BUFFER-1.0",
                        1.0
                );

        System.out.println(
                "[BUFFER-DEGREE] " + earlyExpansion
        );
        System.out.println(
                "[BUFFER-DEGREE] " + middleExpansion
        );
        System.out.println(
                "[BUFFER-DEGREE] " + lateExpansion
        );

        /*
        * 三组实验均处于线程池总容量范围内，
        * 所有任务都应该正常完成。
        */
        assertCompleteWithoutRejection(earlyExpansion, 6);
        assertCompleteWithoutRejection(middleExpansion, 6);
        assertCompleteWithoutRejection(lateExpansion, 6);

        /*
        * 较低的Buffer Degree应更早扩展线程。
        */
        assertTrue(
                earlyExpansion.getPeakPoolSize()
                        > lateExpansion.getPeakPoolSize(),
                "Buffer Degree为0.0时应比1.0更早扩容，"
                        + "并观察到更高的峰值线程数"
        );

        assertTrue(
                earlyExpansion.getPeakPoolSize()
                        >= middleExpansion.getPeakPoolSize(),
                "Buffer Degree为0.0时的峰值线程数"
                        + "不应低于0.5"
        );

        assertTrue(
                middleExpansion.getPeakPoolSize()
                        >= lateExpansion.getPeakPoolSize(),
                "Buffer Degree为0.5时的峰值线程数"
                        + "不应低于1.0"
        );
    }
        /**
     * 验证ABTP在优雅关闭超时后能够：
     *
     * 1. 中断正在运行的任务；
     * 2. 取出队列中尚未执行的任务；
     * 3. 将所有任务归类为失败；
     * 4. 不留下未决任务；
     * 5. 最终正常终止。
     */
    @Test
    @Timeout(value = 6, unit = TimeUnit.SECONDS)
    void shouldForceShutdownAndAccountEveryTaskAfterTimeout()
            throws InterruptedException {

        ThreadPoolExperimentResult result =
                ThreadPoolExperimentRunner.runAbtpExperiment(
                        "ABTP-TIMEOUT",
                        1,
                        1,
                        3,
                        4,
                        5000L,
                        0L,
                        1.0,
                        false,
                        -1,
                        -1.0,
                        10L,
                        100L,
                        3,
                        1L
                );

        System.out.println(
                "[ABTP-TIMEOUT] " + result
        );

        assertEquals(
                4,
                result.getSubmitted()
        );

        assertEquals(
                0,
                result.getSuccess(),
                "5秒任务不应在强制关闭前自然完成"
        );

        assertEquals(
                4,
                result.getFailed(),
                "1个运行任务和3个排队任务均应归类为失败"
        );

        assertEquals(
                0,
                result.getRejected(),
                "线程池总容量足够，不应发生提交拒绝"
        );

        assertEquals(
                0,
                result.getUnresolved(),
                "强制关闭后不允许存在未决任务"
        );

        assertEquals(
                4,
                result.getResolvedTaskCount()
        );

        assertTrue(
                result.isTimedOut(),
                "实验必须明确记录发生了等待超时"
        );

        assertTrue(
                result.isAccountingComplete(),
                "即使超时，所有任务也必须完整记账"
        );

        assertTrue(
                result.isTerminated(),
                "强制关闭后线程池必须正常终止"
        );

        assertTrue(
                result.isRunComplete(),
                "超时不等于实验结果不完整"
        );
    }

    /**
     * 验证提交线程在任务提交间隔中被中断后：
     *
     * 1. 不再继续提交后续任务；
     * 2. InterruptedException能够传播给调用方；
     * 3. 提交线程的中断标记得到恢复；
     * 4. 已创建的ABTP Worker最终退出；
     * 5. 测试不会因为残留线程而无法结束。
     */
    @Test
    @Timeout(value = 8, unit = TimeUnit.SECONDS)
    void shouldPropagateSubmitInterruptionAndReleaseWorkers()
            throws InterruptedException {

        String workerNamePrefix =
                "abtp-interrupt-worker-";

        AtomicInteger workerSequence =
                new AtomicInteger();

        ThreadFactory namedThreadFactory =
                task -> {
                    Thread worker =
                            new Thread(
                                    task,
                                    workerNamePrefix
                                            + workerSequence.incrementAndGet()
                            );

                    /*
                    * 保持非守护线程。
                    *
                    * 若线程池没有正确关闭，
                    * Maven测试进程将可能无法退出，
                    * 更容易暴露线程泄漏。
                    */
                    worker.setDaemon(false);
                    return worker;
                };

        AtomicReference<Throwable> capturedThrowable =
                new AtomicReference<>();

        AtomicBoolean interruptFlagRestored =
                new AtomicBoolean(false);

        Thread experimentThread =
                new Thread(
                        () -> {
                            try {
                                ThreadPoolExperimentRunner
                                        .runAbtpExperiment(
                                                "ABTP-SUBMIT-INTERRUPT",
                                                1,
                                                1,
                                                1,
                                                20,
                                                5000L,
                                                1000L,
                                                1.0,
                                                false,
                                                -1,
                                                -1.0,
                                                10L,
                                                100L,
                                                3,
                                                2L,
                                                namedThreadFactory
                                        );

                                capturedThrowable.set(
                                        new AssertionError(
                                                "提交线程被中断后，"
                                                        + "实验不应正常返回"
                                        )
                                );
                            } catch (InterruptedException interruptedException) {
                                /*
                                * 记录执行器向上抛出异常时，
                                * 当前线程的中断标记是否已经恢复。
                                */
                                interruptFlagRestored.set(
                                        Thread.currentThread()
                                                .isInterrupted()
                                );

                                capturedThrowable.set(
                                        interruptedException
                                );
                            } catch (Throwable throwable) {
                                capturedThrowable.set(
                                        throwable
                                );
                            }
                        },
                        "abtp-interrupt-experiment"
                );

        experimentThread.start();

        /*
        * 等待提交线程进入submitIntervalMs对应的
        * TIMED_WAITING状态。
        *
        * 此时第一个任务通常已经提交，
        * 提交线程正在等待下一次提交。
        */
        waitUntilThreadReachesState(
                experimentThread,
                Thread.State.TIMED_WAITING,
                2000L
        );

        experimentThread.interrupt();

        /*
        * 等待实验调用线程退出。
        */
        experimentThread.join(4000L);

        assertFalse(
                experimentThread.isAlive(),
                "提交线程被中断后必须及时退出"
        );

        Throwable throwable =
                capturedThrowable.get();

        assertNotNull(
                throwable,
                "必须记录实验线程的退出原因"
        );

        assertTrue(
                throwable instanceof InterruptedException,
                "实验执行器必须向调用方传播InterruptedException，"
                        + "实际异常为：" + throwable
        );

        assertTrue(
                interruptFlagRestored.get(),
                "重新抛出InterruptedException前，"
                        + "必须恢复当前线程的中断标记"
        );

        /*
        * 等待被shutdownNow中断的Worker退出。
        */
        waitUntilNoLiveThreadWithPrefix(
                workerNamePrefix,
                3000L
        );
    }
}