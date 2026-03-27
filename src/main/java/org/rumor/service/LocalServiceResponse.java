package org.rumor.service;

/**
 * In-memory implementation of {@link ServiceResponse} for local service invocation.
 * Routes write/close calls to {@link OnStateChange} as {@link RequestEvent} emissions.
 */
class LocalServiceResponse implements ServiceResponse {

    private final OnStateChange onStateChange;
    private boolean closed;

    LocalServiceResponse(OnStateChange onStateChange) {
        this.onStateChange = onStateChange;
    }

    @Override
    public void write(byte[] data) {
        if (closed) throw new IllegalStateException("Response already closed");
        onStateChange.accept(new RequestEvent.StreamData(data));
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        onStateChange.accept(new RequestEvent.Succeeded());
    }

    @Override
    public void fail(byte[] error) {
        if (closed) return;
        closed = true;
        onStateChange.accept(new RequestEvent.Failed(new String(error, java.nio.charset.StandardCharsets.UTF_8)));
    }
}
