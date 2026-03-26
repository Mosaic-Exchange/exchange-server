package org.rumor.service;

/**
 * Events emitted during the lifecycle of an outbound service request.
 *
 * <ul>
 *   <li>{@link Processing} – the request has been sent and is being processed</li>
 *   <li>{@link StreamData} – a chunk of response data has arrived</li>
 *   <li>{@link Succeeded} – the request completed successfully</li>
 *   <li>{@link Failed} – the request failed (includes a reason)</li>
 * </ul>
 */
public sealed interface RequestEvent {

    record Processing() implements RequestEvent {}

    record StreamData(byte[] data) implements RequestEvent {}

    record Succeeded() implements RequestEvent {}

    record Failed(String reason) implements RequestEvent {}
}
