package org.rumor.service;

/**
 * In-memory implementation of {@link ServiceResponse} for local service invocation.
 * Routes write/close calls to {@link OnStateChange} as {@link RequestEvent} emissions.
 *
 * <p>When {@code singleWrite} is {@code true} (used by {@link RService#request}),
 * calling {@link #write(byte[])} more than once throws {@link IllegalStateException},
 * and the data is delivered with the {@link RequestEvent.Succeeded} event on close.
 *
 * <p>When {@code singleWrite} is {@code false} (used by {@link RStreamingService#request}),
 * each {@link #write(byte[])} emits a {@link RequestEvent.StreamData} event.
 */
class LocalServiceResponse implements ServiceResponse {

    private final OnStateChange onStateChange;
    private final boolean singleWrite;
    private boolean closed;
    private boolean written;
    private byte[] bufferedData;

    LocalServiceResponse(OnStateChange onStateChange, boolean singleWrite) {
        this.onStateChange = onStateChange;
        this.singleWrite = singleWrite;
    }

    @Override
    public void write(byte[] data) {
        if (closed) throw new IllegalStateException("Response already closed");
        if (singleWrite && written) {
            throw new IllegalStateException(
                    "write() can only be called once for non-streaming services. " +
                    "Use RStreamingService for multi-write responses.");
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
