package org.rumor.service;

/**
 * Encapsulates an inbound service request: the payload and the
 * {@link ServiceHandle} associated with this invocation.
 *
 * <p>For remote requests, the payload arrives as raw bytes and is decoded
 * lazily on the first call to {@link #data()} using the service's codec.
 * For local requests ({@link RService#request}), the typed object is passed
 * directly — no serialization occurs.
 *
 * <p>Implementations of {@link RService#serve(ServiceRequest, ServiceResponse)}
 * receive this object. Use {@link #isCancelled()} in long-running loops as a
 * cooperative early-exit point — the framework will also throw
 * {@link java.util.concurrent.CancellationException} from the next
 * {@link ServiceResponse#write} call when the request is cancelled.
 *
 * @param <T> the request data type ({@code byte[]} by default)
 */
public class ServiceRequest<T> {

    private final byte[] rawData;
    private final T typedData;
    private final boolean local;
    private final ServiceCodec<T> codec;
    private final ServiceHandle handle;
    private boolean decoded;
    private T decodedData;

    /** Remote constructor: holds raw bytes, decodes lazily via codec. */
    ServiceRequest(byte[] rawData, ServiceHandle handle, ServiceCodec<T> codec) {
        this.rawData = rawData;
        this.typedData = null;
        this.local = false;
        this.codec = codec;
        this.handle = handle;
    }

    /** Local constructor: holds the typed object directly — zero serialization. */
    ServiceRequest(T typedData, ServiceHandle handle) {
        this.rawData = null;
        this.typedData = typedData;
        this.local = true;
        this.codec = null;
        this.handle = handle;
    }

    /**
     * The request payload. For local requests, returns the original typed
     * object with no deserialization. For remote requests, lazily decodes
     * the wire bytes on first access.
     */
    public T data() {
        if (local) return typedData;
        if (!decoded) {
            decodedData = codec.decode(rawData);
            decoded = true;
        }
        return decodedData;
    }

    /**
     * The raw wire bytes. For remote requests, returns the original bytes
     * received over the network. For local byte[] requests, returns the
     * byte array directly. Returns {@code null} for local typed requests.
     *
     * <p>This is a convenience method for byte[] services — use it to avoid
     * casting {@link #data()} in {@code serve()} implementations that use
     * raw (unparameterized) {@code ServiceRequest}.
     */
    public byte[] raw() {
        if (rawData != null) return rawData;
        if (typedData instanceof byte[] b) return b;
        return null;
    }

    /** The handle associated with this request invocation. */
    public ServiceHandle handle() { return handle; }

    /**
     * Returns {@code true} if the caller has cancelled this request.
     * Services should check this in tight loops as a cooperative exit point.
     */
    public boolean isCancelled() { return handle.isCancelled(); }
}
