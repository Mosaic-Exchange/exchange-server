package org.rumor.service;

/**
 * Events emitted during the lifecycle of an outbound service request.
 *
 * <p><b>RService (request/response):</b>
 * <ul>
 *   <li>{@link Processing} – the request has been sent</li>
 *   <li>{@link Succeeded} – completed successfully (includes response data)</li>
 *   <li>{@link Failed} – failed (includes a reason)</li>
 * </ul>
 *
 * <p><b>Streaming ({@link Streamable}):</b>
 * <ul>
 *   <li>{@link Processing} – the request has been sent</li>
 *   <li>{@link StreamData} – a chunk of response data has arrived</li>
 *   <li>{@link Succeeded} – stream completed successfully (no data)</li>
 *   <li>{@link Failed} – failed (includes a reason)</li>
 * </ul>
 */
public sealed interface RequestEvent {

    record Processing() implements RequestEvent {}

    record StreamData(byte[] data) implements RequestEvent {}

    record Succeeded(byte[] data) implements RequestEvent {}

    record Failed(String reason) implements RequestEvent {}
}
