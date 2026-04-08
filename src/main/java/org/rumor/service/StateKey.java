package org.rumor.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method on an {@link DistributedService} as a state-value supplier for the
 * gossip protocol. The method must:
 * <ul>
 *   <li>be {@code public}</li>
 *   <li>take no parameters</li>
 *   <li>return {@link String}</li>
 * </ul>
 *
 * <p>The framework periodically invokes the method and gossips the result
 * under the key {@code ServiceClassName.value()}, where {@code value()} is
 * the annotation's value and {@code ServiceClassName} is the simple name
 * of the concrete {@link DistributedService} subclass.
 *
 * <p>The containing class must also be annotated with {@link MaintainState}.
 *
 * @see MaintainState
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface StateKey {
    /** The unqualified state key name (e.g. {@code "SHARED_FILES"}). */
    String value();
}
