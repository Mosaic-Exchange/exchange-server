package org.rumor.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an {@link RService} as a streaming service.
 *
 * <p>When present, the framework will:
 * <ul>
 *   <li>Run {@link RService#serve} on a dedicated thread (not the Netty I/O thread),
 *       so blocking operations are safe.</li>
 *   <li>Allow {@link ServiceResponse#write(byte[])} to be called multiple times
 *       to stream data in chunks.</li>
 *   <li>Enforce backpressure: {@code write()} blocks when the channel is not writable.</li>
 *   <li>Limit to one active streaming service per node at a time.</li>
 * </ul>
 *
 * <p>Without this annotation, the service uses the default request/response model:
 * {@code serve()} runs on the Netty I/O thread (must not block), and
 * {@code write()} may only be called once.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Streamable {
}
