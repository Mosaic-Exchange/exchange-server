package org.rumor.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an {@link DistributedService} as one that publishes application state into
 * the gossip protocol.
 *
 * <p>When present, the framework scans the service class for methods
 * annotated with {@link StateKey} and periodically calls them to publish
 * their return values into gossip.
 *
 * <p>Each gossip key is internally qualified as
 * {@code ServiceClassName.KEY} to guarantee uniqueness across services.
 *
 * <p>Example:
 * <pre>{@code
 * @MaintainState
 * public class MyService extends RService {
 *
 *     @StateKey("MY_CUSTOM_KEY")
 *     public String computeValue() {
 *         return "hello";
 *     }
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MaintainState {
}
