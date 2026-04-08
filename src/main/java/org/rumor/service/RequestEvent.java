package org.rumor.service;

/**
 * Events emitted during the lifecycle of an outbound service request.
 *
 * <p>The type parameter {@code T} matches the response type of the service
 * ({@code byte[]} by default). Data-carrying events ({@link Succeeded},
 * {@link StreamData}) deliver the typed response payload.
 *
 * <p><b>RService (request/response):</b>
 * <ul>
 *   <li>{@link Processing} – the request has been sent</li>
 *   <li>{@link Succeeded} – completed successfully (includes response data)</li>
 *   <li>{@link Failed} – failed (includes a reason)</li>
 *   <li>{@link Cancelled} – cancelled by the caller via {@link ServiceHandle#cancel()}</li>
 * </ul>
 *
 * <p><b>Streaming ({@link Streamable}):</b>
 * <ul>
 *   <li>{@link Processing} – the request has been sent</li>
 *   <li>{@link StreamData} – a chunk of response data has arrived</li>
 *   <li>{@link Succeeded} – stream completed successfully (no data)</li>
 *   <li>{@link Failed} – failed (includes a reason)</li>
 *   <li>{@link Cancelled} – cancelled by the caller via {@link ServiceHandle#cancel()}</li>
 * </ul>
 *
 * @param <T> the response data type
 */
public sealed interface RequestEvent<T> {

    record Processing<T>() implements RequestEvent<T> {}

    record StreamData<T>(T data) implements RequestEvent<T> {
        /** Convenience accessor that returns data as {@code byte[]}. For byte[] services only. */
        public byte[] raw() { return (byte[]) (Object) data; }
    }

    record Succeeded<T>(T data) implements RequestEvent<T> {
        /** Convenience accessor that returns data as {@code byte[]}. For byte[] services only. */
        public byte[] raw() { return (byte[]) (Object) data; }
    }

    record Failed<T>(String reason) implements RequestEvent<T> {}

    record Cancelled<T>() implements RequestEvent<T> {}
}
