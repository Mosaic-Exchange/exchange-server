package org.rumor.service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;

/**
 * In-memory implementation of {@link ServiceResponse} for local service invocation.
 * Routes write/close calls to {@link OnStateChange} as {@link RequestEvent} emissions.
 *
 * <p>For local requests, the response object is passed directly to the
 * callback — no serialization occurs.
 *
 * <p>When {@code singleWrite} is {@code true} (non-{@link Streamable} services),
 * calling {@link #write} more than once throws {@link IllegalStateException},
 * and the data is delivered with the {@link RequestEvent.Succeeded} event on close.
 *
 * <p>When {@code singleWrite} is {@code false} ({@link Streamable} services),
 * each {@link #write} emits a {@link RequestEvent.StreamData} event.
 *
 * @param <T> the response data type
 */
class LocalServiceResponse<T> implements ServiceResponse<T> {

    private final OnStateChange<T> onStateChange;
    private final boolean singleWrite;
    private final ServiceHandle handle;
    private boolean closed;
    private boolean written;
    private T bufferedData;

    LocalServiceResponse(OnStateChange<T> onStateChange, boolean singleWrite, ServiceHandle handle) {
        this.onStateChange = onStateChange;
        this.singleWrite = singleWrite;
        this.handle = handle;
    }

    @Override
    public void write(T data) {
        doWrite(data);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeRaw(byte[] data) {
        doWrite((T) (Object) data);
    }

    @SuppressWarnings("unchecked")
    private void doWrite(Object data) {
        if (closed) throw new IllegalStateException("Response already closed");
        if (handle != null && handle.isCancelled()) {
            throw new CancellationException("Service request cancelled");
        }
        if (singleWrite && written) {
            throw new IllegalStateException(
                    "write() can only be called once for non-streaming services. " +
                    "Use @Streamable for multi-write responses.");
        }
        written = true;
        if (singleWrite) {
            bufferedData = (T) data;
        } else {
            onStateChange.accept(new RequestEvent.StreamData<>((T) data));
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (singleWrite) {
            onStateChange.accept(new RequestEvent.Succeeded<>(bufferedData));
        } else {
            onStateChange.accept(new RequestEvent.Succeeded<>(null));
        }
    }

    @Override
    public void fail(byte[] error) {
        if (closed) return;
        closed = true;
        onStateChange.accept(new RequestEvent.Failed<>(new String(error, StandardCharsets.UTF_8)));
    }
}
