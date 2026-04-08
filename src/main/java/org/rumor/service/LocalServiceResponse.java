package org.rumor.service;

/**
 * In-memory implementation of {@link ServiceResponse} for local service invocation.
 * Routes write/close calls to {@link OnStateChange} as {@link RequestEvent} emissions.
 *
 * <p>When {@code singleWrite} is {@code true} (non-{@link Streamable} services),
 * calling {@link #write(byte[])} more than once throws {@link IllegalStateException},
 * and the data is delivered with the {@link RequestEvent.Succeeded} event on close.
 *
 * <p>When {@code singleWrite} is {@code false} ({@link Streamable} services),
 * each {@link #write(byte[])} emits a {@link RequestEvent.StreamData} event.
 *
 * <p>If a {@link ServiceHandle} is provided and has been cancelled, the next
 * call to {@link #write(byte[])} throws {@link java.util.concurrent.CancellationException},
 * which the framework catches to emit a {@link RequestEvent.Failed} event.
 */
class LocalServiceResponse implements ServiceResponse {

    private final OnStateChange onStateChange;
    private final boolean singleWrite;
    private final ServiceHandle handle;
    private boolean closed;
    private boolean written;
    private byte[] bufferedData;

    LocalServiceResponse(OnStateChange onStateChange, boolean singleWrite, ServiceHandle handle) {
        this.onStateChange = onStateChange;
        this.singleWrite = singleWrite;
        this.handle = handle;
    }

    @Override
    public void write(byte[] data) {
        if (closed) throw new IllegalStateException("Response already closed");
        if (handle != null && handle.isCancelled()) {
            throw new java.util.concurrent.CancellationException("Service request cancelled");
        }
        if (singleWrite && written) {
            throw new IllegalStateException(
                    "write() can only be called once for non-streaming services. " +
                    "Use @Streamable for multi-write responses.");
        }
        written = true;
        if (singleWrite) {
            bufferedData = data;
        } else {
            onStateChange.accept(new RequestEvent.StreamData(data));
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (singleWrite) {
            onStateChange.accept(new RequestEvent.Succeeded(bufferedData));
        } else {
            onStateChange.accept(new RequestEvent.Succeeded(null));
        }
    }

    @Override
    public void fail(byte[] error) {
        if (closed) return;
        closed = true;
        onStateChange.accept(new RequestEvent.Failed(new String(error, java.nio.charset.StandardCharsets.UTF_8)));
    }
}
