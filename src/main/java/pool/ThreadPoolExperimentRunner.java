package pool;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * 线程池实验执行器。
 *
 * 当前仅支持运行JDK ThreadPoolExecutor实验。
 */
public final class ThreadPoolExperimentRunner {

    private ThreadPoolExperimentRunner() {
        // 工具类不允许实例化
    }

    /**
     * 运行一次JDK线程池实验。
     *
     * @param strategyName       实验策略名称
     * @param corePoolSize       核心线程数
     * @param maximumPoolSize    最大线程数
     * @param queueCapacity      队列容量
     * @param taskCount          提交任务总数
     * @param taskDurationMs     单个任务模拟执行时间
     * @param awaitTimeoutSeconds 等待实验结束的最大时间
     * @return 结构化实验结果
     */
    public static ThreadPoolExperimentResult runJdkExperiment(
        String strategyName,
        int corePoolSize,
        int maximumPoolSize,
        int queueCapacity,
        int taskCount,
        long taskDurationMs,
        long awaitTimeoutSeconds)
        throws InterruptedException {

        return runJdkExperiment(
                strategyName,
                corePoolSize,
                maximumPoolSize,
                queueCapacity,
                taskCount,
                taskDurationMs,
                0L,
                awaitTimeoutSeconds
        );
    }
    /**
     * 使用原子操作记录本轮实验观察到的最大线程数。
     *
     * 多个Worker和提交线程可能同时更新该值，
     * 因此不能使用普通int直接比较后赋值。
     */
    private static void recordPeakPoolSize(
            AtomicInteger peakPoolSize,
            int currentPoolSize) {

        peakPoolSize.accumulateAndGet(
                currentPoolSize,
                Math::max
        );
    }
    public static ThreadPoolExperimentResult runJdkExperiment(
            String strategyName,
            int corePoolSize,
            int maximumPoolSize,
            int queueCapacity,
            int taskCount,
            long taskDurationMs,
            long submitIntervalMs,
            long awaitTimeoutSeconds)
            throws InterruptedException {

        validateArguments(
                strategyName,
                corePoolSize,
                maximumPoolSize,
                queueCapacity,
                taskCount,
                taskDurationMs,
                submitIntervalMs,
                awaitTimeoutSeconds
        );

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failedCount = new AtomicInteger();
        AtomicInteger rejectedCount = new AtomicInteger();
        AtomicInteger peakPoolSize = new AtomicInteger();

        CountDownLatch taskLatch =
                new CountDownLatch(taskCount);

        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        60L,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(queueCapacity),
                        (task, threadPoolExecutor) -> {
                            rejectedCount.incrementAndGet();
                            taskLatch.countDown();
                        }
                );

        long experimentStartNs = System.nanoTime();
        long submitStartNs = System.nanoTime();

        try {
            for (int taskIndex = 0;
                taskIndex < taskCount;
                taskIndex++) {

                executor.execute(() -> {
                    /*
                    * Worker实际开始运行任务时，再观察一次当前线程数。
                    */
                    recordPeakPoolSize(
                            peakPoolSize,
                            executor.getPoolSize()
                    );
                    try {
                        Thread.sleep(taskDurationMs);
                        successCount.incrementAndGet();
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                        failedCount.incrementAndGet();
                    } catch (RuntimeException runtimeException) {
                        failedCount.incrementAndGet();
                    } finally {
                        taskLatch.countDown();
                    }
                });
                recordPeakPoolSize(
                        peakPoolSize,
                        executor.getPoolSize()
                );

                /*
                * 最后一个任务提交后无需继续等待。
                */
                if (submitIntervalMs > 0
                        && taskIndex < taskCount - 1) {

                    Thread.sleep(submitIntervalMs);
                }
            }
        } catch (InterruptedException interruptedException) {
            /*
            * 提交线程被中断时，不允许线程池继续在后台运行。
            */
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw interruptedException;
        }

        long submitEndNs = System.nanoTime();

        /*
        * 停止接收新任务，让已经接收的任务继续执行。
        */
        executor.shutdown();

        /*
        * 第一层等待：观察所有任务能否在规定时间内获得明确结果。
        */
        boolean tasksResolvedWithinTimeout =
                taskLatch.await(
                        awaitTimeoutSeconds,
                        TimeUnit.SECONDS
                );

        /*
        * 第二层等待：观察线程池是否能够自然结束。
        */
        boolean terminatedGracefully =
                executor.awaitTermination(
                        awaitTimeoutSeconds,
                        TimeUnit.SECONDS
                );

        /*
        * 任意一个正常等待阶段超时，都记录为timedOut。
        *
        * 即使后续shutdownNow清理成功，
        * timedOut仍然保留为true，用于说明实验经历过异常关闭。
        */
        boolean timedOut =
                !tasksResolvedWithinTimeout
                        || !terminatedGracefully;

        boolean terminated =
                terminatedGracefully;

        if (!terminatedGracefully) {
            /*
            * 中断正在运行的Worker，并取出队列中尚未开始的任务。
            */
            List<Runnable> abandonedTasks =
                    executor.shutdownNow();

            /*
            * shutdownNow返回的任务从未进入Worker，
            * 不会执行任务内部的finally，因此必须在这里手动记账。
            */
            for (Runnable ignored : abandonedTasks) {
                failedCount.incrementAndGet();
                taskLatch.countDown();
            }

            /*
            * 等待运行中的任务响应中断并退出。
            */
            terminated =
                    executor.awaitTermination(
                            awaitTimeoutSeconds,
                            TimeUnit.SECONDS
                    );
        }

        long experimentEndNs = System.nanoTime();

        int success = successCount.get();
        int failed = failedCount.get();
        int rejected = rejectedCount.get();

        long remainingTaskCount = taskLatch.getCount();

        int unresolved =
                remainingTaskCount > Integer.MAX_VALUE
                        ? Integer.MAX_VALUE
                        : (int) remainingTaskCount;

        int resolved =
                success + failed + rejected;

        boolean accounted =
                unresolved == 0
                        && resolved == taskCount;

        long submitMs =
                TimeUnit.NANOSECONDS.toMillis(
                        submitEndNs - submitStartNs
                );

        long totalMs =
                TimeUnit.NANOSECONDS.toMillis(
                        experimentEndNs - experimentStartNs
                );

        return new ThreadPoolExperimentResult(
                strategyName,
                taskCount,
                success,
                failed,
                rejected,
                unresolved,
                0,
                peakPoolSize.get(),
                submitMs,
                totalMs,
                accounted,
                terminated,
                timedOut
        );
    }

            /**
         * 运行一次AdaptiveBufferedThreadPoolExecutor实验。
         *
         * @param strategyName        策略名称
         * @param corePoolSize        核心线程数
         * @param maximumPoolSize     最大线程数
         * @param queueCapacity       队列容量
         * @param taskCount           提交任务数
         * @param taskDurationMs      单任务执行时间
         * @param submitIntervalMs    相邻任务提交间隔
         * @param bufferDegree        队列缓冲水位
         * @param preventRejection    是否启用强制入队
         * @param threadLoadJudge     提交压力阈值
         * @param cpuLoadJudge        CPU负载阈值
         * @param waitTime            初始退避等待参数
         * @param timeout             阻塞入队超时
         * @param maxRetryAttempts    最大退避重试次数
         * @param awaitTimeoutSeconds 实验等待超时
         * @return 结构化实验结果
         */
        public static ThreadPoolExperimentResult runAbtpExperiment(
            String strategyName,
            int corePoolSize,
            int maximumPoolSize,
            int queueCapacity,
            int taskCount,
            long taskDurationMs,
            long submitIntervalMs,
            double bufferDegree,
            boolean preventRejection,
            int threadLoadJudge,
            double cpuLoadJudge,
            long waitTime,
            long timeout,
            int maxRetryAttempts,
            long awaitTimeoutSeconds)
            throws InterruptedException {

        return runAbtpExperiment(
                strategyName,
                corePoolSize,
                maximumPoolSize,
                queueCapacity,
                taskCount,
                taskDurationMs,
                submitIntervalMs,
                bufferDegree,
                preventRejection,
                threadLoadJudge,
                cpuLoadJudge,
                waitTime,
                timeout,
                maxRetryAttempts,
                awaitTimeoutSeconds,
                Executors.defaultThreadFactory()
        );
    }
    public static ThreadPoolExperimentResult runAbtpExperiment(
        String strategyName,
        int corePoolSize,
        int maximumPoolSize,
        int queueCapacity,
        int taskCount,
        long taskDurationMs,
        long submitIntervalMs,
        double bufferDegree,
        boolean preventRejection,
        int threadLoadJudge,
        double cpuLoadJudge,
        long waitTime,
        long timeout,
        int maxRetryAttempts,
        long awaitTimeoutSeconds,
        ThreadFactory threadFactory)
        throws InterruptedException {

            validateArguments(
                    strategyName,
                    corePoolSize,
                    maximumPoolSize,
                    queueCapacity,
                    taskCount,
                    taskDurationMs,
                    submitIntervalMs,
                    awaitTimeoutSeconds
            );

            validateAbtpArguments(
                    bufferDegree,
                    waitTime,
                    timeout,
                    maxRetryAttempts
            );

            if (threadFactory == null) {
                throw new IllegalArgumentException(
                        "threadFactory must not be null"
                );
            }

            AtomicInteger successCount = new AtomicInteger();
            AtomicInteger failedCount = new AtomicInteger();
            AtomicInteger rejectedCount = new AtomicInteger();
            AtomicInteger peakPoolSize = new AtomicInteger();

            CountDownLatch taskLatch =
                    new CountDownLatch(taskCount);

            /*
            * 注意：这里使用项目自定义的RejectedExecutionHandler，
            * 不是java.util.concurrent包中的同名接口。
            */
            RejectedExecutionHandler rejectionHandler =
                    (task, threadPoolExecutor) -> {
                        rejectedCount.incrementAndGet();
                        taskLatch.countDown();
                    };

            AdaptiveBufferedThreadPoolExecutor executor =
                    new AdaptiveBufferedThreadPoolExecutor(
                            corePoolSize,
                            maximumPoolSize,
                            60L,
                            TimeUnit.SECONDS,
                            new ArrayBlockingQueue<>(queueCapacity),
                            threadFactory,
                            rejectionHandler,
                            bufferDegree,
                            preventRejection,
                            threadLoadJudge,
                            cpuLoadJudge,
                            waitTime,
                            timeout,
                            maxRetryAttempts
                    );

            long experimentStartNs = System.nanoTime();
            long submitStartNs = System.nanoTime();

            try {
                for (int taskIndex = 0;
                    taskIndex < taskCount;
                    taskIndex++) {

                    executor.execute(() -> {
                        recordPeakPoolSize(
                                peakPoolSize,
                                executor.getPoolSize()
                        );

                        try {
                            Thread.sleep(taskDurationMs);
                            successCount.incrementAndGet();
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                            failedCount.incrementAndGet();
                        } catch (RuntimeException runtimeException) {
                            failedCount.incrementAndGet();
                        } finally {
                            taskLatch.countDown();
                        }
                    });

                    if (submitIntervalMs > 0
                            && taskIndex < taskCount - 1) {

                        Thread.sleep(submitIntervalMs);
                    }
                }
            }  catch (InterruptedException interruptedException) {
            /*
            * Thread.sleep抛出InterruptedException时，
            * 当前线程的中断标记会被清除。
            *
            * 这里先完成线程池清理，再恢复中断标记并向上抛出。
            */

            List<Runnable> abandonedTasks =
                    executor.shutdownNow();

            /*
            * 队列中尚未进入Worker的任务不会执行任务finally，
            * 因此补充失败记账。
            *
            * 当前方法最终会抛出异常，不会返回实验结果，
            * 但保持内部记账一致有利于后续排查。
            */
            for (Runnable ignored : abandonedTasks) {
                failedCount.incrementAndGet();
                taskLatch.countDown();
            }

            /*
            * 等待已经启动的Worker响应中断并退出。
            *
            * 此时InterruptedException已经清除了原中断标记，
            * 因此可以正常调用awaitTermination。
            */
            try {
                executor.awaitTermination(
                        awaitTimeoutSeconds,
                        TimeUnit.SECONDS
                );
            } catch (InterruptedException cleanupInterruptedException) {
                /*
                * 清理期间再次收到中断时，再发送一次shutdownNow。
                * 最终仍传播最初的提交中断异常。
                */
                executor.shutdownNow();
            } finally {
                /*
                * 恢复调用线程的中断语义。
                */
                Thread.currentThread().interrupt();
            }

            throw interruptedException;
        }

            long submitEndNs = System.nanoTime();

            /*
            * 停止接收新任务，让已经接收的任务继续执行。
            */
            executor.shutdown();

            /*
            * 第一层等待：观察所有任务能否在规定时间内获得明确结果。
            */
            boolean tasksResolvedWithinTimeout =
                    taskLatch.await(
                            awaitTimeoutSeconds,
                            TimeUnit.SECONDS
                    );

            /*
            * 第二层等待：观察线程池是否能够自然结束。
            */
            boolean terminatedGracefully =
                    executor.awaitTermination(
                            awaitTimeoutSeconds,
                            TimeUnit.SECONDS
                    );

            /*
            * 任意一个正常等待阶段超时，都记录为timedOut。
            *
            * 即使后续shutdownNow清理成功，
            * timedOut仍然保留为true，用于说明实验经历过异常关闭。
            */
            boolean timedOut =
                    !tasksResolvedWithinTimeout
                            || !terminatedGracefully;

            boolean terminated =
                    terminatedGracefully;

            if (!terminatedGracefully) {
                /*
                * 中断正在运行的Worker，并取出队列中尚未开始的任务。
                */
                List<Runnable> abandonedTasks =
                        executor.shutdownNow();

                /*
                * shutdownNow返回的任务从未进入Worker，
                * 不会执行任务内部的finally，因此必须在这里手动记账。
                */
                for (Runnable ignored : abandonedTasks) {
                    failedCount.incrementAndGet();
                    taskLatch.countDown();
                }

                /*
                * 等待运行中的任务响应中断并退出。
                */
                terminated =
                        executor.awaitTermination(
                                awaitTimeoutSeconds,
                                TimeUnit.SECONDS
                        );
            }

            long experimentEndNs = System.nanoTime();

            int success = successCount.get();
            int failed = failedCount.get();
            int rejected = rejectedCount.get();

            long remainingTaskCount = taskLatch.getCount();

            int unresolved =
                    remainingTaskCount > Integer.MAX_VALUE
                            ? Integer.MAX_VALUE
                            : (int) remainingTaskCount;

            int resolved =
                    success + failed + rejected;

            boolean accounted =
                    unresolved == 0
                            && resolved == taskCount;

            long submitMs =
                    TimeUnit.NANOSECONDS.toMillis(
                            submitEndNs - submitStartNs
                    );

            long totalMs =
                    TimeUnit.NANOSECONDS.toMillis(
                            experimentEndNs - experimentStartNs
                    );

            return new ThreadPoolExperimentResult(
                    strategyName,
                taskCount,
                success,
                failed,
                rejected,
                unresolved,
                executor.getForceEnqueueCount(),
                peakPoolSize.get(),
                submitMs,
                totalMs,
                accounted,
                terminated,
                timedOut
        );
        }
    private static void validateArguments(
            String strategyName,
            int corePoolSize,
            int maximumPoolSize,
            int queueCapacity,
            int taskCount,
            long taskDurationMs,
            long submitIntervalMs,
            long awaitTimeoutSeconds) {

        if (strategyName == null
                || strategyName.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "strategyName must not be blank"
            );
        }

        if (corePoolSize <= 0) {
            throw new IllegalArgumentException(
                    "corePoolSize must be greater than 0"
            );
        }

        if (maximumPoolSize < corePoolSize) {
            throw new IllegalArgumentException(
                    "maximumPoolSize must be greater than "
                            + "or equal to corePoolSize"
            );
        }

        if (queueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "queueCapacity must be greater than 0"
            );
        }

        if (taskCount <= 0) {
            throw new IllegalArgumentException(
                    "taskCount must be greater than 0"
            );
        }

        if (taskDurationMs < 0) {
            throw new IllegalArgumentException(
                    "taskDurationMs must not be negative"
            );
        }

        if (submitIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "submitIntervalMs must not be negative"
            );
        }


        if (awaitTimeoutSeconds <= 0) {
            throw new IllegalArgumentException(
                    "awaitTimeoutSeconds must be greater than 0"
            );
        }
    }

        private static void validateAbtpArguments(
            double bufferDegree,
            long waitTime,
            long timeout,
            int maxRetryAttempts) {

        if (Double.isNaN(bufferDegree)
                || Double.isInfinite(bufferDegree)
                || bufferDegree < 0.0
                || bufferDegree > 1.0) {

            throw new IllegalArgumentException(
                    "bufferDegree must be between 0.0 and 1.0"
            );
        }

        if (waitTime < 0) {
            throw new IllegalArgumentException(
                    "waitTime must not be negative"
            );
        }

        if (timeout < 0) {
            throw new IllegalArgumentException(
                    "timeout must not be negative"
            );
        }

        if (maxRetryAttempts < 0) {
            throw new IllegalArgumentException(
                    "maxRetryAttempts must not be negative"
            );
        }
    }
}