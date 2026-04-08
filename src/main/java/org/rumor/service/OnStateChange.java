package org.rumor.service;

/**
 * Callback for service request lifecycle events.
 *
 * @param <T> the response data type (matches the service's response type)
 */
@FunctionalInterface
public interface OnStateChange<T> {
    void accept(RequestEvent<T> event);
}
