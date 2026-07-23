package pool;

/**
 * 单次线程池实验的结果对象。
 *
 * 只保存实验结果，不负责执行任务、打印日志或写入报告。
 */
public final class ThreadPoolExperimentResult {

    private final String strategy;
    private final int submitted;
    private final int success;
    private final int failed;
    private final int rejected;
    private final int unresolved;
    private final int forceEnqueueCount;
        /**
     * 实验执行过程中观察到的最大线程池线程数。
     */
    private final int peakPoolSize;
    private final long submitMs;
    private final long totalMs;
    private final boolean accounted;
    private final boolean terminated;
    /**
     * 实验是否超过正常等待时间，并进入过超时清理流程。
     *
     * timedOut为true不代表最终一定没有终止。
     * 强制关闭成功时可以同时满足：
     * timedOut=true、terminated=true。
     */
    private final boolean timedOut;

    public ThreadPoolExperimentResult(
            String strategy,
            int submitted,
            int success,
            int failed,
            int rejected,
            int unresolved,
            int forceEnqueueCount,
            long submitMs,
            long totalMs,
            boolean accounted,
            boolean terminated) {

        this(
                strategy,
                submitted,
                success,
                failed,
                rejected,
                unresolved,
                forceEnqueueCount,
                0,
                submitMs,
                totalMs,
                accounted,
                terminated
        );
    }

    public ThreadPoolExperimentResult(
            String strategy,
            int submitted,
            int success,
            int failed,
            int rejected,
            int unresolved,
            int forceEnqueueCount,
            int peakPoolSize,
            long submitMs,
            long totalMs,
            boolean accounted,
            boolean terminated) {
        this(
                    strategy,
                    submitted,
                    success,
                    failed,
                    rejected,
                    unresolved,
                    forceEnqueueCount,
                    peakPoolSize,
                    submitMs,
                    totalMs,
                    accounted,
                    terminated,
                    false
            );
    }
    public ThreadPoolExperimentResult(
        String strategy,
        int submitted,
        int success,
        int failed,
        int rejected,
        int unresolved,
        int forceEnqueueCount,
        int peakPoolSize,
        long submitMs,
        long totalMs,
        boolean accounted,
        boolean terminated,
        boolean timedOut) {

        if (strategy == null || strategy.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "strategy must not be blank"
            );
        }

        if (submitted < 0
                || success < 0
                || failed < 0
                || rejected < 0
                || unresolved < 0
                || forceEnqueueCount < 0
                || peakPoolSize < 0
                || submitMs < 0
                || totalMs < 0) {

            throw new IllegalArgumentException(
                    "experiment metrics must not be negative"
            );
        }

        this.strategy = strategy;
        this.submitted = submitted;
        this.success = success;
        this.failed = failed;
        this.rejected = rejected;
        this.unresolved = unresolved;
        this.forceEnqueueCount = forceEnqueueCount;
        this.peakPoolSize = peakPoolSize;
        this.submitMs = submitMs;
        this.totalMs = totalMs;
        this.accounted = accounted;
        this.terminated = terminated;
        this.timedOut = timedOut;
    }

    public String getStrategy() {
        return strategy;
    }

    public int getSubmitted() {
        return submitted;
    }

    public int getSuccess() {
        return success;
    }

    public int getFailed() {
        return failed;
    }

    public int getRejected() {
        return rejected;
    }

    public int getUnresolved() {
        return unresolved;
    }

    public int getForceEnqueueCount() {
        return forceEnqueueCount;
    }

    public int getPeakPoolSize() {
        return peakPoolSize;
    }

    public long getSubmitMs() {
        return submitMs;
    }

    public long getTotalMs() {
        return totalMs;
    }

    public boolean isAccounted() {
        return accounted;
    }

    public boolean isTerminated() {
        return terminated;
    }
    public boolean isTimedOut() {
        return timedOut;
    }

    /**
     * 已经得到明确结果的任务数量。
     */
    public int getResolvedTaskCount() {
        return success + failed + rejected;
    }

    /**
     * 所有任务是否都被成功、失败或拒绝三类结果完整记账。
     */
    public boolean isAccountingComplete() {
        return accounted
                && unresolved == 0
                && getResolvedTaskCount() == submitted;
    }

    /**
     * 实验是否完整结束。
     */
    public boolean isRunComplete() {
        return isAccountingComplete() && terminated;
    }

    /**
     * 当前实验中的任务成功完成比例。
     */
    public double getCompletionRate() {
        if (submitted == 0) {
            return 0.0;
        }

        return success / (double) submitted;
    }

    @Override
    public String toString() {
        return "ThreadPoolExperimentResult{"
                + "strategy='" + strategy + '\''
                + ", submitted=" + submitted
                + ", success=" + success
                + ", failed=" + failed
                + ", rejected=" + rejected
                + ", unresolved=" + unresolved
                + ", forceEnqueueCount=" + forceEnqueueCount
                + ", peakPoolSize=" + peakPoolSize
                + ", submitMs=" + submitMs
                + ", totalMs=" + totalMs
                + ", accounted=" + accounted
                + ", terminated=" + terminated
                + ", timedOut=" + timedOut
                + '}';
    }
}