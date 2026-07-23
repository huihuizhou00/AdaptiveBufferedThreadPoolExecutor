package pool;

public interface RejectedExecutionHandler {
    void rejectedExecution(Runnable r, AdaptiveBufferedThreadPoolExecutor executor);
}
