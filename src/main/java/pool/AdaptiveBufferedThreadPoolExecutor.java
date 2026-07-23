package pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AdaptiveBufferedThreadPoolExecutor extends AbstractExecutorService {

    private static final Logger log = LoggerFactory.getLogger(AdaptiveBufferedThreadPoolExecutor.class);
    private final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));
    private static final int COUNT_BITS = Integer.SIZE - 3;
    private static final int CAPACITY   = (1 << COUNT_BITS) - 1;

    // runState is stored in the high-order bits
    private static final int RUNNING    = -1 << COUNT_BITS;
    private static final int SHUTDOWN   =  0 << COUNT_BITS;
    private static final int STOP       =  1 << COUNT_BITS;
    private static final int TIDYING    =  2 << COUNT_BITS;
    private static final int TERMINATED =  3 << COUNT_BITS;

    // Packing and unpacking ctl
    private static int runStateOf(int c)     { return c & ~CAPACITY; }
    private static int workerCountOf(int c)  { return c & CAPACITY; }
    private static int ctlOf(int rs, int wc) { return rs | wc; }

    //防止拒绝策略发生
    private boolean preventRejection = true;

    private static boolean runStateLessThan(int c, int s) {
        return c < s;
    }

    private static boolean runStateAtLeast(int c, int s) {
        return c >= s;
    }

    private static boolean isRunning(int c) {
        return c < SHUTDOWN;
    }

    private boolean isPreventRejection() {return preventRejection;}

    private boolean compareAndIncrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect + 1);
    }


    private boolean compareAndDecrementWorkerCount(int expect) {
        return ctl.compareAndSet(expect, expect - 1);
    }

    private void decrementWorkerCount() {
        do {} while (! compareAndDecrementWorkerCount(ctl.get()));
    }

    private final BlockingQueue<Runnable> workQueue;

    private final ReentrantLock mainLock = new ReentrantLock();

    private final HashSet<Worker> workers = new HashSet<Worker>();

    private final Condition termination = mainLock.newCondition();

    private int largestPoolSize;

    private long completedTaskCount;

    private volatile ThreadFactory threadFactory;

    private volatile RejectedExecutionHandler handler;

    private volatile long keepAliveTime;

    private volatile boolean allowCoreThreadTimeOut;

    private volatile int corePoolSize;

    private volatile int maximumPoolSize;

    private static final RuntimePermission shutdownPerm =
            new RuntimePermission("modifyThread");

    private final AccessControlContext acc;

    private int maxRetryAttempts;  // 最大重试次数

    private Integer threadLoadJudge;

    private double cpuLoadJudge;

    private volatile AtomicInteger threadLoad = new AtomicInteger(0);

    private final AtomicInteger forceEnqueueCount = new AtomicInteger(0);

    public long waitTime;  // 初始空转等待时间，单位是纳秒

    private long timeout;  // 阻塞等待时间，由用户自定义

    private double bufferDegree;

    private BasicCalculate basicCalculate;

    private final class Worker
            extends AbstractQueuedSynchronizer
            implements Runnable
    {
        private static final long serialVersionUID = 6138294804551838833L;

        final Thread thread;
        Runnable firstTask;
        volatile long completedTasks;

        Worker(Runnable firstTask) {
            setState(-1); // inhibit interrupts until runWorker
            this.firstTask = firstTask;
            this.thread = getThreadFactory().newThread(this);
        }

        public void run() {
            runWorker(this);
        }

        protected boolean isHeldExclusively() {
            return getState() != 0;
        }

        protected boolean tryAcquire(int unused) {
            if (compareAndSetState(0, 1)) {
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }
            return false;
        }

        protected boolean tryRelease(int unused) {
            setExclusiveOwnerThread(null);
            setState(0);
            return true;
        }

        public void lock()        { acquire(1); }
        public boolean tryLock()  { return tryAcquire(1); }
        public void unlock()      { release(1); }
        public boolean isLocked() { return isHeldExclusively(); }

        void interruptIfStarted() {
            Thread t;
            if (getState() >= 0 && (t = thread) != null && !t.isInterrupted()) {
                try {
                    t.interrupt();
                } catch (SecurityException ignore) {
                }
            }
        }
    }

    private void advanceRunState(int targetState) {
        for (;;) {
            int c = ctl.get();
            if (runStateAtLeast(c, targetState) ||
                    ctl.compareAndSet(c, ctlOf(targetState, workerCountOf(c))))
                break;
        }
    }

    final void tryTerminate() {
        for (;;) {
            int c = ctl.get();
            if (isRunning(c) ||
                    runStateAtLeast(c, TIDYING) ||
                    (runStateOf(c) == SHUTDOWN && ! workQueue.isEmpty()))
                return;
            if (workerCountOf(c) != 0) { // Eligible to terminate
                interruptIdleWorkers(ONLY_ONE);
                return;
            }

            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                if (ctl.compareAndSet(c, ctlOf(TIDYING, 0))) {
                    try {
                        terminated();
                    } finally {
                        ctl.set(ctlOf(TERMINATED, 0));
                        termination.signalAll();
                    }
                    return;
                }
            } finally {
                mainLock.unlock();
            }
            // else retry on failed CAS
        }
    }

    private void checkShutdownAccess() {
        SecurityManager security = System.getSecurityManager();
        if (security != null) {
            security.checkPermission(shutdownPerm);
            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                for (Worker w : workers)
                    security.checkAccess(w.thread);
            } finally {
                mainLock.unlock();
            }
        }
    }

    private void interruptWorkers() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            for (Worker w : workers)
                w.interruptIfStarted();
        } finally {
            mainLock.unlock();
        }
    }

    private void interruptIdleWorkers(boolean onlyOne) {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            for (Worker w : workers) {
                Thread t = w.thread;
                if (!t.isInterrupted() && w.tryLock()) {
                    try {
                        t.interrupt();
                    } catch (SecurityException ignore) {
                    } finally {
                        w.unlock();
                    }
                }
                if (onlyOne)
                    break;
            }
        } finally {
            mainLock.unlock();
        }
    }

    private void interruptIdleWorkers() {
        interruptIdleWorkers(false);
    }

    private static final boolean ONLY_ONE = true;

    final void reject(Runnable command) {
        handler.rejectedExecution(command, this);
    }

    void onShutdown() {
    }

    final boolean isRunningOrShutdown(boolean shutdownOK) {
        int rs = runStateOf(ctl.get());
        return rs == RUNNING || (rs == SHUTDOWN && shutdownOK);
    }

    private List<Runnable> drainQueue() {
        BlockingQueue<Runnable> q = workQueue;
        ArrayList<Runnable> taskList = new ArrayList<Runnable>();
        q.drainTo(taskList);
        if (!q.isEmpty()) {
            for (Runnable r : q.toArray(new Runnable[0])) {
                if (q.remove(r))
                    taskList.add(r);
            }
        }
        return taskList;
    }

    private boolean addWorker(Runnable firstTask, boolean core) {
        retry:
        for (;;) {
            int c = ctl.get();
            int rs = runStateOf(c);

            // Check if queue empty only if necessary.
            if (rs >= SHUTDOWN &&
                    ! (rs == SHUTDOWN &&
                            firstTask == null &&
                            ! workQueue.isEmpty()))
                return false;

            for (;;) {
                int wc = workerCountOf(c);
                if (wc >= CAPACITY ||
                        wc >= (core ? corePoolSize : maximumPoolSize))
                    return false;
                if (compareAndIncrementWorkerCount(c))
                    break retry;
                c = ctl.get();  // Re-read ctl
                if (runStateOf(c) != rs)
                    continue retry;
                // else CAS failed due to workerCount change; retry inner loop
            }
        }

        boolean workerStarted = false;
        boolean workerAdded = false;
        Worker w = null;
        try {
            w = new Worker(firstTask);
            final Thread t = w.thread;
            if (t != null) {
                final ReentrantLock mainLock = this.mainLock;
                mainLock.lock();
                try {
                    // Recheck while holding lock.
                    // Back out on ThreadFactory failure or if
                    // shut down before lock acquired.
                    int rs = runStateOf(ctl.get());

                    if (rs < SHUTDOWN ||
                            (rs == SHUTDOWN && firstTask == null)) {
                        if (t.isAlive()) // precheck that t is startable
                            throw new IllegalThreadStateException();
                        workers.add(w);
                        int s = workers.size();
                        if (s > largestPoolSize)
                            largestPoolSize = s;
                        workerAdded = true;
                    }
                } finally {
                    mainLock.unlock();
                }
                if (workerAdded) {
                    t.start();
                    workerStarted = true;
                }
            }
        } finally {
            if (! workerStarted)
                addWorkerFailed(w);
        }
        return workerStarted;
    }

    private void addWorkerFailed(Worker w) {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            if (w != null)
                workers.remove(w);
            decrementWorkerCount();
            tryTerminate();
        } finally {
            mainLock.unlock();
        }
    }

    private void processWorkerExit(Worker w, boolean completedAbruptly) {
        if (completedAbruptly)
            decrementWorkerCount();

        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            completedTaskCount += w.completedTasks;
            workers.remove(w);
        } finally {
            mainLock.unlock();
        }

        tryTerminate();

        int c = ctl.get();
        if (runStateLessThan(c, STOP)) {
            if (!completedAbruptly) {
                int min = allowCoreThreadTimeOut ? 0 : corePoolSize;
                if (min == 0 && !workQueue.isEmpty())
                    min = 1;
                if (workerCountOf(c) >= min)
                    return;
            }
            addWorker(null, false);
        }
    }

    private Runnable getTask() {
        boolean timedOut = false;

        for (;;) {
            int c = ctl.get();
            int rs = runStateOf(c);

            if (rs >= SHUTDOWN && (rs >= STOP || workQueue.isEmpty())) {
                decrementWorkerCount();
                return null;
            }

            int wc = workerCountOf(c);
            boolean timed = allowCoreThreadTimeOut || wc > corePoolSize;

            if ((wc > maximumPoolSize || (timed && timedOut))
                    && (wc > 1 || workQueue.isEmpty())) {
                if (compareAndDecrementWorkerCount(c))
                    return null;
                continue;
            }

            try {
                Runnable r = timed ?
                        workQueue.poll(keepAliveTime, TimeUnit.NANOSECONDS) :
                        workQueue.take();
                if (r != null)
                    return r;
                timedOut = true;
            } catch (InterruptedException retry) {
                timedOut = false;
            }
        }
    }

    final void runWorker(Worker w) {
        Thread wt = Thread.currentThread();
        Runnable task = w.firstTask;
        w.firstTask = null;
        w.unlock();
        boolean completedAbruptly = true;
        try {
            while (task != null || (task = getTask()) != null) {
                w.lock();
                if ((runStateAtLeast(ctl.get(), STOP) ||
                        (Thread.interrupted() &&
                                runStateAtLeast(ctl.get(), STOP))) &&
                        !wt.isInterrupted())
                    wt.interrupt();
                try {
                    beforeExecute(wt, task);
                    Throwable thrown = null;
                    try {
                        task.run();
                    } catch (RuntimeException x) {
                        thrown = x; throw x;
                    } catch (Error x) {
                        thrown = x; throw x;
                    } catch (Throwable x) {
                        thrown = x; throw new Error(x);
                    } finally {
                        afterExecute(task, thrown);
                    }
                } finally {
                    task = null;
                    w.completedTasks++;
                    w.unlock();
                }
            }
            completedAbruptly = false;
        } finally {
            processWorkerExit(w, completedAbruptly);
        }
    }

    public AdaptiveBufferedThreadPoolExecutor(int corePoolSize,
                                              int maximumPoolSize,
                                              long keepAliveTime,
                                              TimeUnit unit,
                                              BlockingQueue<Runnable> workQueue,
                                              ThreadFactory threadFactory,
                                              RejectedExecutionHandler handler,
                                              double bufferDegree) {
        if (corePoolSize < 0 ||
                maximumPoolSize <= 0 ||
                maximumPoolSize < corePoolSize ||
                keepAliveTime < 0)
            throw new IllegalArgumentException();
        if (workQueue == null || threadFactory == null || handler == null)
            throw new NullPointerException();
        this.acc = System.getSecurityManager() == null ?
                null :
                AccessController.getContext();
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.workQueue = workQueue;
        this.bufferDegree = bufferDegree;
        this.keepAliveTime = unit.toNanos(keepAliveTime);
        this.threadFactory = threadFactory;
        this.handler = handler;
        this.preventRejection = false;
    }

    public AdaptiveBufferedThreadPoolExecutor(int corePoolSize,
                                              int maximumPoolSize,
                                              long keepAliveTime,
                                              TimeUnit unit,
                                              BlockingQueue<Runnable> workQueue,
                                              ThreadFactory threadFactory,
                                              RejectedExecutionHandler handler,
                                              double bufferDegree,
                                              boolean isPreventRejection,
                                              Integer threadLoadJudge,
                                              double cpuLoadJudge,
                                              long waitTime,
                                              long timeout,
                                              Integer maxRetryAttempts) {
        if (corePoolSize < 0 ||
                maximumPoolSize <= 0 ||
                maximumPoolSize < corePoolSize ||
                keepAliveTime < 0)
            throw new IllegalArgumentException();
        if (workQueue == null || threadFactory == null || handler == null)
            throw new NullPointerException();
        this.acc = System.getSecurityManager() == null ?
                null :
                AccessController.getContext();
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.workQueue = workQueue;
        this.keepAliveTime = unit.toNanos(keepAliveTime);
        this.threadFactory = threadFactory;
        this.handler = handler;
        this.bufferDegree = bufferDegree;
        this.preventRejection = isPreventRejection;
        this.threadLoadJudge = threadLoadJudge;
        this.cpuLoadJudge = cpuLoadJudge;
        this.waitTime = waitTime;
        this.timeout = timeout;
        this.maxRetryAttempts = maxRetryAttempts;
        this.basicCalculate = new BasicCalculate();
    }

     public int getForceEnqueueCount() {
        
        return forceEnqueueCount.get();
    }

    public void execute(Runnable command) {
        if (command == null)
            throw new NullPointerException();  // 如果命令为空，则抛出空指针异常

        int c = ctl.get();  // 获取当前线程池的状态

        // Step 1: 如果当前线程池中的线程数小于核心线程数，则创建新线程
        if (workerCountOf(c) < corePoolSize) {
            if (addWorker(command, true)) {  // 尝试添加工作线程，且允许新线程加入
                threadLoadDecrement(0);  // 线程负载自减
                return;  // 如果添加成功，直接返回
            }
            c = ctl.get();  // 重新获取控制状态，以便检查状态变化
        }

        //step2
        if (workQueue.size() <= (workQueue.remainingCapacity() + workQueue.size()) * bufferDegree) {
            if (isRunning(c) && workQueue.offer(command)) {  // 如果线程池在运行状态且队列有空间，加入任务
                threadLoadDecrement(0);  // 线程负载自减
                int recheck = ctl.get();  // 重新检查线程池状态
                // 如果队列添加任务成功，继续检查并可能增加工作线程
                if (!isRunning(recheck) && remove(command)) {
                    reject(command);  // 如果线程池已停止，则移除任务并拒绝执行
                } else if (workerCountOf(recheck) == 0) {
                    addWorker(null, false);  // 如果没有可用的工作线程，则增加一个空工作线程来保持队列不被阻塞
                }
                return;  // 任务成功加入队列，返回
            }
        }

        // Step 3: 如果线程数小于等于最大线程数，则尝试创建新线程
        if (workerCountOf(c) <= maximumPoolSize) {
            if (addWorker(command, false)) {  // 尝试添加工作线程，但不强制要求添加
                threadLoadDecrement(0);  // 线程负载自减
                return;  // 如果添加成功，直接返回
            }
        }

        // Step 4: 如果线程数已满，尝试将任务加入队列，如果队列已满，则执行拒绝策略
        if (!workQueue.offer(command)) {  // 如果队列满了，返回false，表示任务没有成功加入队列
            if (!forceEnqueue(command, ctl.get())) {
                threadLoad.incrementAndGet();  // 线程负载自增
                reject(command);  // 执行任务拒绝操作，通常是抛出异常或其他处理
            }
        }
        threadLoadDecrement(0);  // 线程负载自减
    }

    public void threadLoadDecrement(int minValue) {
    while (true) {
        int current = threadLoad.get();

        if (current <= minValue) {
            return;
        }

        if (threadLoad.compareAndSet(
                current,
                current - 1
        )) {
            return;
        }
    }
}

    public boolean forceEnqueue(Runnable command, int c) {

        if (!isPreventRejection()) {
            return false;
        }

        //计算强制入队次数，判断拒绝策略有效性
        forceEnqueueCount.incrementAndGet();

        // 计算CPU负载度：总工作时间 / 总时间（此处假设已获取CPU的使用率）
        double cpuLoad = basicCalculate.getCPULoad();

        threadLoad.incrementAndGet();  // 线程负载自增

        if (this.threadLoad.get() > threadLoadJudge && cpuLoad >= cpuLoadJudge) {
            // 线程池负载较高，CPU负载较高，尝试入队一次
            return workQueue.offer(command);  // 尝试入队一次
        }

        // 判断是否需要进入等待状态或重试
        if ((this.threadLoad.get() <= threadLoadJudge)) {
            // 线程池负载较低,阻塞等待并重试入队
            return blockAndRetry(command);
        } else if (this.threadLoad.get() > threadLoadJudge && cpuLoad < cpuLoadJudge) {
            // 线程池负载较高，CPU负载较低，CPU空转并等待重试
            return spinAndRetry(command);
        }

        return false;  // 默认返回失败
    }


    // 阻塞等待并重试入队的方法（直接使用 put 方法）
    private boolean blockAndRetry(Runnable command) {
        try {
            // 使用 put 方法，自动阻塞直到队列有空闲空间
            return this.workQueue.offer(command, timeout, TimeUnit.MILLISECONDS);  // 阻塞直到队列有空间
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // 处理中断
        }
        //可能导致任务静默丢失（已修改）
        return false;
    }

    // CPU空转并等待重试的方法（使用指数退避算法）
    private boolean spinAndRetry(Runnable command) {
    int retryCount = 0;

    // 每次任务提交拥有独立的退避时间
    long currentWaitNanos = 10_000L;  // 10微秒
    long maxSpinNanos = 50_000L;      // 最大50微秒

    while (retryCount < 3) {
        long startTime = System.nanoTime();

        // 只进行极短时间空转
        while (System.nanoTime() - startTime < currentWaitNanos) {
            // Java 8没有Thread.onSpinWait()
        }

        if (workQueue.offer(command)) {
            return true;
        }

        retryCount++;

        currentWaitNanos = Math.min(
                currentWaitNanos * 2,
                maxSpinNanos
        );
    }

    // 短自旋仍然失败，改为限时阻塞，避免继续消耗CPU
    try {
        return workQueue.offer(
                command,
                1,
                TimeUnit.MILLISECONDS
        );
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
    }
}

    public static class BasicCalculate {

        private final SystemInfo systemInfo = new SystemInfo();
        private final CentralProcessor processor = systemInfo.getHardware().getProcessor();
        private volatile double CPULoadCache = 0.0;
        private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        public BasicCalculate() {
            scheduler.scheduleAtFixedRate(this::updateCPULoad, 0, 5, TimeUnit.SECONDS);
        }

        // 获取系统 CPU 负载（0.0 ~ 1.0）
        public void updateCPULoad() {
            // 获取上一次 CPU Tick（用于计算差值）
            long[] prevTicks = processor.getSystemCpuLoadTicks();
            try {
                Thread.sleep(1000); // 睡眠1秒以获取采样差值
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ;
            }
            long[] ticks = processor.getSystemCpuLoadTicks();
            CPULoadCache = processor.getSystemCpuLoadBetweenTicks(prevTicks);
        }

        public double getCPULoad() {
            // 获取上一次 CPU Tick（用于计算差值）
            return CPULoadCache;
        }

        public void shutdown() {
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown(); // 或者 scheduler.shutdown();
            }
        }
    }

    //非阻塞强制入队
    public Boolean forceEnqueueOnceWithNone(Runnable command, int c) {
        if (isRunning(c) && this.workQueue.offer(command)) {
            int recheck = ctl.get();
            if (!isRunning(recheck) && remove(command)) {
                reject(command);
                return Boolean.FALSE;
            } else if (workerCountOf(recheck) == 0) {
                addWorker(null, false);
            }
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    //阻塞强制入队
    public Boolean forceEnqueueWithBlock(Runnable command, int c) {
        if (isRunning(c)) {
            try {
                this.workQueue.put(command); // 使用 put 方法等待直到能入队
                int recheck = ctl.get();
                if (!isRunning(recheck) && remove(command)) {
                    reject(command);
                    return Boolean.FALSE;
                } else if (workerCountOf(recheck) == 0) {
                    addWorker(null, false);
                }
                return Boolean.TRUE;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 恢复中断状态
                reject(command);
                return Boolean.FALSE;
            }
        }
        return Boolean.FALSE;
    }

    public void shutdown() {
    final ReentrantLock mainLock = this.mainLock;
    mainLock.lock();
    try {
        checkShutdownAccess();
        advanceRunState(SHUTDOWN);
        interruptIdleWorkers();
        onShutdown();
    } finally {
        mainLock.unlock();
    }
    if (basicCalculate != null) {
        basicCalculate.shutdown();
    }
    tryTerminate();
}

    public List<Runnable> shutdownNow() {
        List<Runnable> tasks;
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            checkShutdownAccess();
            advanceRunState(STOP);
            interruptWorkers();
            tasks = drainQueue();
        } finally {
            mainLock.unlock();
        }
        tryTerminate();
        return tasks;
    }

    public boolean isShutdown() {
        return ! isRunning(ctl.get());
    }

    public boolean isTerminating() {
        int c = ctl.get();
        return ! isRunning(c) && runStateLessThan(c, TERMINATED);
    }

    public boolean isTerminated() {
        return runStateAtLeast(ctl.get(), TERMINATED);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            for (;;) {
                if (runStateAtLeast(ctl.get(), TERMINATED))
                    return true;
                if (nanos <= 0)
                    return false;
                nanos = termination.awaitNanos(nanos);
            }
        } finally {
            mainLock.unlock();
        }
    }

    protected void finalize() {
        SecurityManager sm = System.getSecurityManager();
        if (sm == null || acc == null) {
            shutdown();
        } else {
            PrivilegedAction<Void> pa = () -> { shutdown(); return null; };
            AccessController.doPrivileged(pa, acc);
        }
    }

    public void setThreadFactory(ThreadFactory threadFactory) {
        if (threadFactory == null)
            throw new NullPointerException();
        this.threadFactory = threadFactory;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
        if (handler == null)
            throw new NullPointerException();
        this.handler = handler;
    }

    public RejectedExecutionHandler getRejectedExecutionHandler() {
        return handler;
    }

    public void setCorePoolSize(int corePoolSize) {
        if (corePoolSize < 0)
            throw new IllegalArgumentException();
        int delta = corePoolSize - this.corePoolSize;
        this.corePoolSize = corePoolSize;
        if (workerCountOf(ctl.get()) > corePoolSize)
            interruptIdleWorkers();
        else if (delta > 0) {
            int k = Math.min(delta, workQueue.size());
            while (k-- > 0 && addWorker(null, true)) {
                if (workQueue.isEmpty())
                    break;
            }
        }
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public boolean prestartCoreThread() {
        return workerCountOf(ctl.get()) < corePoolSize &&
                addWorker(null, true);
    }

    void ensurePrestart() {
        int wc = workerCountOf(ctl.get());
        if (wc < corePoolSize)
            addWorker(null, true);
        else if (wc == 0)
            addWorker(null, false);
    }

    public int prestartAllCoreThreads() {
        int n = 0;
        while (addWorker(null, true))
            ++n;
        return n;
    }

    public static class DiscardPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, AdaptiveBufferedThreadPoolExecutor executor) {

        }
    }

    public static class CountPolicy implements RejectedExecutionHandler {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public void rejectedExecution(Runnable r, AdaptiveBufferedThreadPoolExecutor executor) {
            count.incrementAndGet();
        }

        public int getCount() {
            return count.get();
        }
    }


    public boolean allowsCoreThreadTimeOut() {
        return allowCoreThreadTimeOut;
    }

    public void allowCoreThreadTimeOut(boolean value) {
        if (value && keepAliveTime <= 0)
            throw new IllegalArgumentException("Core threads must have nonzero keep alive times");
        if (value != allowCoreThreadTimeOut) {
            allowCoreThreadTimeOut = value;
            if (value)
                interruptIdleWorkers();
        }
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        if (maximumPoolSize <= 0 || maximumPoolSize < corePoolSize)
            throw new IllegalArgumentException();
        this.maximumPoolSize = maximumPoolSize;
        if (workerCountOf(ctl.get()) > maximumPoolSize)
            interruptIdleWorkers();
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setKeepAliveTime(long time, TimeUnit unit) {
        if (time < 0)
            throw new IllegalArgumentException();
        if (time == 0 && allowsCoreThreadTimeOut())
            throw new IllegalArgumentException("Core threads must have nonzero keep alive times");
        long keepAliveTime = unit.toNanos(time);
        long delta = keepAliveTime - this.keepAliveTime;
        this.keepAliveTime = keepAliveTime;
        if (delta < 0)
            interruptIdleWorkers();
    }

    public long getKeepAliveTime(TimeUnit unit) {
        return unit.convert(keepAliveTime, TimeUnit.NANOSECONDS);
    }

    public BlockingQueue<Runnable> getQueue() {
        return workQueue;
    }

    public boolean remove(Runnable task) {
        boolean removed = workQueue.remove(task);
        tryTerminate(); // In case SHUTDOWN and now empty
        return removed;
    }

    public void purge() {
        final BlockingQueue<Runnable> q = workQueue;
        try {
            Iterator<Runnable> it = q.iterator();
            while (it.hasNext()) {
                Runnable r = it.next();
                if (r instanceof Future<?> && ((Future<?>)r).isCancelled())
                    it.remove();
            }
        } catch (ConcurrentModificationException fallThrough) {
            for (Object r : q.toArray())
                if (r instanceof Future<?> && ((Future<?>)r).isCancelled())
                    q.remove(r);
        }

        tryTerminate();
    }

    public int getPoolSize() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            return runStateAtLeast(ctl.get(), TIDYING) ? 0 : workers.size();
        } finally {
            mainLock.unlock();
        }
    }

    public int getActiveCount() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            int n = 0;
            for (Worker w : workers)
                if (w.isLocked())
                    ++n;
            return n;
        } finally {
            mainLock.unlock();
        }
    }

    public int getLargestPoolSize() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            return largestPoolSize;
        } finally {
            mainLock.unlock();
        }
    }

    public long getTaskCount() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            long n = completedTaskCount;
            for (Worker w : workers) {
                n += w.completedTasks;
                if (w.isLocked())
                    ++n;
            }
            return n + workQueue.size();
        } finally {
            mainLock.unlock();
        }
    }

    public long getCompletedTaskCount() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            long n = completedTaskCount;
            for (Worker w : workers)
                n += w.completedTasks;
            return n;
        } finally {
            mainLock.unlock();
        }
    }

    public String toString() {
        long ncompleted;
        int nworkers, nactive;
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            ncompleted = completedTaskCount;
            nactive = 0;
            nworkers = workers.size();
            for (Worker w : workers) {
                ncompleted += w.completedTasks;
                if (w.isLocked())
                    ++nactive;
            }
        } finally {
            mainLock.unlock();
        }
        int c = ctl.get();
        String rs = (runStateLessThan(c, SHUTDOWN) ? "Running" : (runStateAtLeast(c, TERMINATED) ? "Terminated" : "Shutting down"));
        return super.toString() + "[" + rs + ", pool size = " + nworkers + ", active threads = " + nactive + ", queued tasks = " + workQueue.size() + ", completed tasks = " + ncompleted + "]";
    }

    protected void beforeExecute(Thread t, Runnable r) {}

    protected void afterExecute(Runnable r, Throwable t) {}

    protected void terminated() {}

}
