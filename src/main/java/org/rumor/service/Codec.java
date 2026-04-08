package org.rumor.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a custom {@link ServiceCodec} for a {@link ServiceRequest} or
 * {@link ServiceResponse} parameter on a {@link DistributedService#serve} method.
 *
 * <p>Place this annotation on the parameters of your {@code serve()} override
 * to control how request/response data is serialized for remote calls:
 *
 * <pre>{@code
 * @Override
 * public void serve(
 *     @Codec(MyReqCodec.class)  ServiceRequest<MyReq>  request,
 *     @Codec(MyRespCodec.class) ServiceResponse<MyResp> response) {
 *     ...
 * }
 * }</pre>
 *
 * <p>When absent, the framework uses a default codec: identity passthrough
 * for {@code byte[]} types, or Java serialization for other types.
 *
 * <p>The referenced codec class must have a public no-arg constructor.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Codec {
    Class<? extends ServiceCodec<?>> value();
}
