package org.rumor.service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handle returned by {@link DistributedService#dispatch} and {@link DistributedService#request}
 * that allows the caller to cancel an in-progress service invocation.
 *
 * <p>For local requests ({@link DistributedService#request}), cancellation is
 * cooperative: the next {@link ServiceResponse#write} checks the
 * cancelled flag and throws, unwinding the {@code serve()} call.
 *
 * <p>For remote dispatches ({@link DistributedService#dispatch}), the framework
 * sends a {@code SERVICE_CANCEL} frame to the remote peer and immediately
 * delivers a {@link RequestEvent.Failed} event to the caller.
 *
 * <p>Each handle is unique to a single {@code dispatch()} or
 * {@code request()} invocation, so concurrent calls naturally get
 * independent handles.
 */
public class ServiceHandle {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile Runnable onCancel;

    /**
     * Cancel the associated service invocation.
     *
     * <p>Calling this more than once is harmless (only the first call
     * takes effect).
     */
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            Runnable hook = onCancel;
            if (hook != null) hook.run();
        }
    }

    /**
     * Returns {@code true} if {@link #cancel()} has been called.
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Package-private: the framework sets this to wire the actual
     * cancellation behavior (e.g. sending SERVICE_CANCEL for remote
     * dispatches). If the handle is already cancelled when the hook
     * is set, the hook fires immediately.
     */
    void onCancel(Runnable hook) {
        this.onCancel = hook;
        if (cancelled.get() && hook != null) {
            hook.run();
        }
    }
}
