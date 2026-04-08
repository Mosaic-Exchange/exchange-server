package org.rumor.service;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Base class for services in the Rumor framework.
 *
 * <p>Extend this class and implement {@link #serve(ServiceRequest, ServiceResponse)} to handle
 * incoming requests. Use {@link #dispatch} to invoke on a remote peer,
 * or {@link #request} for local execution.
 *
 * <p>By default, request and response data is raw {@code byte[]}. For the default case,
 * extend with raw types ({@code extends RService}) and override {@code serve()} with raw
 * {@code ServiceRequest} / {@code ServiceResponse} parameters. For custom types, extend
 * with concrete types ({@code extends RService<MyReq, MyResp>}) and override with typed
 * parameters.
 *
 * <p>The framework discovers types from the {@code serve()} signature at registration
 * time and handles serialization/deserialization automatically:
 * <ul>
 *   <li>Identity passthrough for {@code byte[]} (zero-cost)</li>
 *   <li>Java serialization for other types when no custom codec is specified</li>
 *   <li>Custom codecs via {@link Codec} annotations on {@code serve()} parameters</li>
 * </ul>
 *
 * <p><b>Local requests ({@link #request}) bypass serialization entirely</b> —
 * the typed object is passed directly to {@code serve()}.
 *
 * <p><b>Default (request/response) mode:</b>
 * <ul>
 *   <li>{@code serve()} runs on the Netty I/O thread — do not block.</li>
 *   <li>{@link ServiceResponse#write} may be called at most once.</li>
 * </ul>
 *
 * <p><b>Streaming mode</b> (annotate the subclass with {@link Streamable}):
 * <ul>
 *   <li>{@code serve()} runs on a dedicated thread — blocking is safe.</li>
 *   <li>{@link ServiceResponse#write} may be called multiple times.</li>
 *   <li>Backpressure is enforced: {@code write()} blocks when the channel
 *       is not writable.</li>
 * </ul>
 *
 * <p><b>Concurrency control</b> (optional, configured via
 * {@link org.rumor.node.Rumor#register(RService, Config)}):
 * <ul>
 *   <li>A {@link Config} can be supplied per-service or globally for all services.</li>
 *   <li>Per-service config takes precedence over the global config.</li>
 *   <li>Remote (inbound) and local requests are served by separate pools,
 *       preventing remote floods from starving local callers.</li>
 *   <li>When a pool is at capacity, the request is rejected immediately (fail-fast).</li>
 * </ul>
 *
 * <p>The service is identified by the simple class name of the subclass.
 *
 * <p>Optionally annotate the subclass with {@link MaintainState} and mark
 * methods with {@link StateKey} to automatically publish application state
 * into the gossip protocol.
 *
 * @param <Req>  the request data type ({@code byte[]} when extending raw)
 * @param <Resp> the response data type ({@code byte[]} when extending raw)
 */
public abstract class RService<Req, Resp> {

    //  Concurrency configuration

    /**
     * Bounded-concurrency configuration for a service's remote and local executor pools.
     *
     * <p>Pass to {@link org.rumor.node.Rumor#register(RService, Config)} for per-service
     * configuration, or to {@link org.rumor.node.Rumor#globalServiceConfig(Config)} to
     * apply the same pools to all services that do not have their own config.
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

    //  Fields

    private ServiceManager manager;
    private ThreadPoolExecutor remoteExecutor;
    private ThreadPoolExecutor localExecutor;
    private boolean ownsExecutors;

    /** Codec for encoding/decoding the request type. Set during registration. */
    @SuppressWarnings("rawtypes")
    ServiceCodec requestCodec;

    /** Codec for encoding/decoding the response type. Set during registration. */
    @SuppressWarnings("rawtypes")
    ServiceCodec responseCodec;

    //  Abstract API

    /**
     * Handle an incoming request.
     *
     * <p>For default (non-streaming) services, {@link ServiceResponse#write}
     * may be called at most once. For {@link Streamable} services, it may be called
     * multiple times to stream data.
     *
     * <p>Check {@link ServiceRequest#isCancelled()} in tight loops for cooperative
     * early exit. The framework will also throw {@link CancellationException} from
     * the next {@link ServiceResponse#write} call when cancelled.
     *
     * @param request  the incoming request (payload + cancellation handle)
     * @param response write response data here; call fail() on error
     */
    public abstract void serve(ServiceRequest<Req> request, ServiceResponse<Resp> response);

    //  Public dispatch API (remote)

    /**
     * Dispatches a request to a remote peer offering this service.
     *
     * @param request       the request object (byte[] by default, or your custom type)
     * @param onStateChange called when request events occur
     * @return a handle that can be used to cancel the request
     */
    public ServiceHandle dispatch(Req request, OnStateChange<Resp> onStateChange) {
        return dispatch(request, onStateChange, null);
    }

    /**
     * Dispatches a request to a remote peer offering this service,
     * filtered by application state criteria.
     *
     * @param request       the request object (byte[] by default, or your custom type)
     * @param onStateChange called when request events occur
     * @param peerFilter    optional predicate over the peer's app state;
     *                      {@code null} means no extra filtering
     * @return a handle that can be used to cancel the request
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public ServiceHandle dispatch(Req request, OnStateChange<Resp> onStateChange,
                         Predicate<Map<String, String>> peerFilter) {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        byte[] encoded = requestCodec.encode(request);

        OnStateChange<byte[]> rawCallback = event -> {
            if (event instanceof RequestEvent.Succeeded s) {
                byte[] data = s.raw();
                Object decoded = data != null ? responseCodec.decode(data) : null;
                onStateChange.accept(new RequestEvent.Succeeded(decoded));
            } else if (event instanceof RequestEvent.StreamData sd) {
                onStateChange.accept(new RequestEvent.StreamData(responseCodec.decode(sd.raw())));
            } else if (event instanceof RequestEvent.Processing) {
                onStateChange.accept(new RequestEvent.Processing());
            } else if (event instanceof RequestEvent.Failed f) {
                onStateChange.accept(new RequestEvent.Failed(f.reason()));
            } else if (event instanceof RequestEvent.Cancelled) {
                onStateChange.accept(new RequestEvent.Cancelled());
            }
        };

        ServiceHandle handle = new ServiceHandle();
        manager.sendRequest(serviceName(), encoded, rawCallback, peerFilter, handle);
        return handle;
    }

    //  Public local API

    /**
     * Invokes this service locally with the same event-driven interface as
     * {@link #dispatch}.
     *
     * <p><b>No serialization occurs for local calls.</b> The request object is
     * passed directly to {@link #serve}, and response objects flow directly
     * to the callback.
     *
     * @param requestData   request object passed directly to {@link #serve}
     * @param onStateChange called when request events occur (same contract as dispatch)
     * @return a handle that can be used to cancel the request
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public ServiceHandle request(Req requestData, OnStateChange<Resp> onStateChange) {
        ServiceHandle handle = new ServiceHandle();
        ServiceRequest request = new ServiceRequest(requestData, handle);
        if (localExecutor != null) {
            try {
                localExecutor.execute(() -> {
                    onStateChange.accept(new RequestEvent.Processing());
                    boolean singleWrite = !this.getClass().isAnnotationPresent(Streamable.class);
                    LocalServiceResponse response = new LocalServiceResponse(onStateChange, singleWrite, handle);
                    try {
                        serve(request, response);
                        response.close();
                    } catch (CancellationException e) {
                        onStateChange.accept(new RequestEvent.Cancelled());
                    } catch (Exception e) {
                        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                        response.fail(msg.getBytes(StandardCharsets.UTF_8));
                    }
                });
            } catch (RejectedExecutionException e) {
                onStateChange.accept(new RequestEvent.Failed("Service at capacity (local)"));
            }
        } else {
            onStateChange.accept(new RequestEvent.Processing());
            boolean singleWrite = !this.getClass().isAnnotationPresent(Streamable.class);
            LocalServiceResponse response = new LocalServiceResponse(onStateChange, singleWrite, handle);
            try {
                serve(request, response);
                response.close();
            } catch (CancellationException e) {
                onStateChange.accept(new RequestEvent.Cancelled());
            } catch (Exception e) {
                response.fail(e.getMessage().getBytes(StandardCharsets.UTF_8));
            }
        }
        return handle;
    }

    //  Framework-internal: executor-aware serve routing

    @SuppressWarnings({"rawtypes", "unchecked"})
    final void executeServe(ServiceRequest request, ServiceResponse response) {
        if (remoteExecutor != null) {
            Future<?> future;
            try {
                future = remoteExecutor.submit(() -> {
                    try {
                        serve(request, response);
                    } catch (CancellationException e) {
                        throw e;
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
                if (e.getCause() instanceof CancellationException ce) throw ce;
                response.fail("Internal error".getBytes(StandardCharsets.UTF_8));
            }
        } else {
            serve(request, response);
        }
    }

    //  Framework-internal: codec resolution

    @SuppressWarnings({"rawtypes", "unchecked"})
    void resolveCodecs() {
        Method serveMethod = findServeMethod(this.getClass());
        if (serveMethod == null) {
            this.requestCodec = ByteArrayCodec.INSTANCE;
            this.responseCodec = ByteArrayCodec.INSTANCE;
            return;
        }

        Parameter[] params = serveMethod.getParameters();
        this.requestCodec = resolveCodec(params[0]);
        this.responseCodec = resolveCodec(params[1]);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ServiceCodec resolveCodec(Parameter param) {
        Codec ann = param.getAnnotation(Codec.class);
        if (ann != null) {
            try {
                return (ServiceCodec) ann.value().getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate codec: " + ann.value().getName()
                        + ". Ensure it has a public no-arg constructor.", e);
            }
        }
        Type paramType = param.getParameterizedType();
        if (paramType instanceof ParameterizedType pt) {
            Type typeArg = pt.getActualTypeArguments()[0];
            if (typeArg == byte[].class) {
                return ByteArrayCodec.INSTANCE;
            }
            return JavaSerializationCodec.INSTANCE;
        }
        return ByteArrayCodec.INSTANCE;
    }

    private static Method findServeMethod(Class<?> clazz) {
        for (Class<?> c = clazz; c != null && c != RService.class; c = c.getSuperclass()) {
            for (Method m : c.getDeclaredMethods()) {
                if ("serve".equals(m.getName()) && m.getParameterCount() == 2 && !m.isBridge()) {
                    return m;
                }
            }
        }
        return null;
    }

    //  Framework-internal: executor injection

    void initLocalExecutors(Config config) {
        this.remoteExecutor = buildExecutor("remote", config.remoteThreads(), config.remoteQueueCapacity());
        this.localExecutor  = buildExecutor("local",  config.localThreads(),  config.localQueueCapacity());
        this.ownsExecutors = true;
    }

    void setSharedExecutors(ThreadPoolExecutor remote, ThreadPoolExecutor local) {
        this.remoteExecutor = remote;
        this.localExecutor  = local;
        this.ownsExecutors  = false;
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

    //  Public accessors

    protected ClusterView clusterView() {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        return manager;
    }

    public final String serviceName() {
        return this.getClass().getSimpleName();
    }

    public final boolean isStreamable() {
        return this.getClass().isAnnotationPresent(Streamable.class);
    }

    public final boolean hasExecutors() {
        return remoteExecutor != null;
    }

    public ThreadPoolExecutor remoteExecutor() { return remoteExecutor; }
    public ThreadPoolExecutor localExecutor() { return localExecutor; }

    public void shutdownExecutors() {
        if (ownsExecutors) {
            if (remoteExecutor != null) remoteExecutor.shutdown();
            if (localExecutor  != null) localExecutor.shutdown();
        }
    }

    protected final String qualifiedKey(String rawKey) {
        return serviceName() + "." + rawKey;
    }

    void setManager(ServiceManager manager) {
        this.manager = manager;
    }
}
