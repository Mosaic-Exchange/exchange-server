package org.rumor.service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extension of {@link RService} that enforces bounded concurrency with
 * separate thread pools for remote and local requests.
 *
 * <p>This prevents remote request floods from starving local callers.
 * Each pool has a configurable thread count and queue capacity. When a
 * pool is at capacity, the request is rejected immediately (fail-fast).
 *
 * <p>Subclasses implement {@link #doServe(byte[], ServiceResponse)} instead
 * of {@link #serve(byte[], ServiceResponse)}. The same contract applies:
 * write response data via {@link ServiceResponse#write(byte[])}, and the
 * framework ensures {@code close()} is called.
 *
 * <p>Example usage:
 * <pre>{@code
 * @Streamable
 * public class MyService extends RPriorityService {
 *
 *     public MyService() {
 *         super(new Config()
 *             .remoteThreads(2)
 *             .remoteQueueCapacity(5)
 *             .localThreads(3)
 *             .localQueueCapacity(5));
 *     }
 *
 *     @Override
 *     protected void doServe(byte[] request, ServiceResponse response) {
 *         // your logic here
 *     }
 * }
 * }</pre>
 */
public abstract class RPriorityService extends RService {

    /**
     * Configuration for the remote and local executor pools.
     *
     * <p>Use the builder-style setters to customise, then pass to the
     * {@link RPriorityService#RPriorityService(Config)} constructor.
     */
    public static class Config {
        private int remoteThreads = 2;
        private int remoteQueueCapacity = 0;
        private int localThreads = 2;
        private int localQueueCapacity = 0;

        /** Number of threads for executing remote (inbound) requests. */
        public Config remoteThreads(int val) { this.remoteThreads = val; return this; }

        /**
         * Queue capacity for remote requests waiting for a thread.
         * {@code 0} means pure fail-fast (no queuing).
         */
        public Config remoteQueueCapacity(int val) { this.remoteQueueCapacity = val; return this; }

        /** Number of threads for executing local requests. */
        public Config localThreads(int val) { this.localThreads = val; return this; }

        /**
         * Queue capacity for local requests waiting for a thread.
         * {@code 0} means pure fail-fast (no queuing).
         */
        public Config localQueueCapacity(int val) { this.localQueueCapacity = val; return this; }

        public int remoteThreads() { return remoteThreads; }
        public int remoteQueueCapacity() { return remoteQueueCapacity; }
        public int localThreads() { return localThreads; }
        public int localQueueCapacity() { return localQueueCapacity; }
    }

    private final ThreadPoolExecutor remoteExecutor;
    private final ThreadPoolExecutor localExecutor;

    protected RPriorityService(Config config) {
        this.remoteExecutor = buildExecutor("remote", config.remoteThreads, config.remoteQueueCapacity);
        this.localExecutor = buildExecutor("local", config.localThreads, config.localQueueCapacity);
    }

    protected RPriorityService() {
        this(new Config());
    }

    private ThreadPoolExecutor buildExecutor(String label, int threads, int queueCapacity) {
        AtomicInteger counter = new AtomicInteger(1);
        BlockingQueue<Runnable> queue = queueCapacity > 0
                ? new ArrayBlockingQueue<>(queueCapacity)
                : new SynchronousQueue<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                threads, threads,
                60L, TimeUnit.SECONDS,
                queue,
                r -> {
                    Thread t = new Thread(r, serviceName() + "-" + label + "-" + counter.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Handle an incoming request. Implement this instead of
     * {@link #serve(byte[], ServiceResponse)}.
     *
     * <p>The same contract as {@code serve()} applies: write response data
     * via {@link ServiceResponse#write(byte[])}, and optionally call
     * {@link ServiceResponse#close()} when done. The framework guarantees
     * {@code close()} is called even if you don't.
     *
     * @param request  the full request bytes
     * @param response write response bytes here; call fail() on error
     */
    protected abstract void doServe(byte[] request, ServiceResponse response);

    /**
     * Submits the request to the remote executor pool. Blocks the calling
     * thread until the executor task completes, preserving the synchronous
     * contract that {@link org.rumor.service.ServiceManager} expects.
     *
     * <p>If the pool and its queue are full, the request is rejected
     * immediately with a failure response.
     */
    @Override
    public final void serve(byte[] request, ServiceResponse response) {
        Future<?> future;
        try {
            future = remoteExecutor.submit(() -> {
                try {
                    doServe(request, response);
                } catch (Exception e) {
                    String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                    response.fail(msg.getBytes(StandardCharsets.UTF_8));
                }
            });
        } catch (RejectedExecutionException e) {
            response.fail("Service at capacity (remote)".getBytes(StandardCharsets.UTF_8));
            return;
        }

        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            response.fail("Interrupted".getBytes(StandardCharsets.UTF_8));
        } catch (ExecutionException e) {
            response.fail("Internal error".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Submits the request to the local executor pool. Returns immediately;
     * results are delivered via the {@link OnStateChange} callback.
     *
     * <p>If the pool and its queue are full, the request is rejected
     * immediately with a {@link RequestEvent.Failed} event.
     */
    @Override
    public final ServiceHandle request(byte[] request, OnStateChange onStateChange) {
        ServiceHandle handle = new ServiceHandle();
        try {
            localExecutor.execute(() -> {
                onStateChange.accept(new RequestEvent.Processing());
                boolean singleWrite = !this.getClass().isAnnotationPresent(Streamable.class);
                LocalServiceResponse response = new LocalServiceResponse(onStateChange, singleWrite, handle);
                try {
                    doServe(request, response);
                    response.close();
                } catch (Exception e) {
                    String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                    response.fail(msg.getBytes(StandardCharsets.UTF_8));
                }
            });
        } catch (RejectedExecutionException e) {
            onStateChange.accept(new RequestEvent.Failed("Service at capacity (local)"));
        }
        return handle;
    }

    /** Returns the remote executor for metrics/debug access. */
    public ThreadPoolExecutor remoteExecutor() { return remoteExecutor; }

    /** Returns the local executor for metrics/debug access. */
    public ThreadPoolExecutor localExecutor() { return localExecutor; }

    /** Shuts down both executor pools. */
    public void shutdownExecutors() {
        remoteExecutor.shutdown();
        localExecutor.shutdown();
    }
}
