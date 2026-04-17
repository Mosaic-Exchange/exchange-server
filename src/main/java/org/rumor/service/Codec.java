package org.rumor.service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a custom {@link ServiceCodec} for a {@link ServiceRequest} or
 * {@link ServiceResponse} parameter on a {@link DistributedService#serve}
 * method.
 *
 * Place this annotation on the parameters of your {@code serve()} override
 * to control how request/response data is serialized for remote calls:
 *
 * <pre>
 * {@code
 * &#64;Override
 * public void serve(
 *     &#64;Codec(MyReqCodec.class)  ServiceRequest<MyReq>  request,
 *     @Codec(MyRespCodec.class) ServiceResponse<MyResp> response) {
 *     ...
 * }
 * }
 * </pre>
 *
 * When absent, the framework uses a default codec: identity passthrough
 * for {@code byte[]} types, or Java serialization for other types.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Codec {
    Class<? extends ServiceCodec<?>> value();
}
