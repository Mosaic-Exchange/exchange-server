package org.rumor.service;

/**
 * Base class for all services in the Rumor framework.
 *
 * <p>Extend this class and implement {@link #serve(byte[], ServiceResponse)} to handle
 * incoming requests from remote peers. Use {@link #dispatch(byte[], OnStateChange)}
 * to invoke this service on a remote peer that offers it.
 *
 * <p>{@code serve()} is called directly on the framework's I/O thread.
 * For non-trivial processing, delegate the work to your own executor
 * to keep the framework responsive.
 *
 * <p>The service is identified by the simple class name of the subclass.
 */
public abstract class RService {

    private ServiceManager manager;

    /**
     * Handle an incoming request from a remote peer.
     *
     * @param request  the full request bytes from the remote peer
     * @param response write response bytes here; call close() when done
     */
    public abstract void serve(byte[] request, ServiceResponse response);

    /**
     * Dispatches a request to a remote peer offering this service.
     * The framework picks a suitable live node automatically.
     *
     * @param request       request bytes sent to the remote peer
     * @param onStateChange called when request events occur:
     *                      {@link RequestEvent.Processing} – request sent,
     *                      {@link RequestEvent.StreamData} – a chunk of response data arrived,
     *                      {@link RequestEvent.Succeeded} – completed successfully,
     *                      {@link RequestEvent.Failed} – failed (includes reason)
     */
    public void dispatch(byte[] request, OnStateChange onStateChange) {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        manager.sendRequest(serviceName(), request, onStateChange);
    }

    /**
     * Invokes this service locally with the same event-driven interface as
     * {@link #dispatch(byte[], OnStateChange)}.
     *
     * <p>The {@link #serve(byte[], ServiceResponse)} method is called on the
     * caller's thread. If that is undesirable, wrap this call in your own executor.
     *
     * @param request       request bytes passed directly to {@link #serve}
     * @param onStateChange called when request events occur (same contract as dispatch)
     */
    public void request(byte[] request, OnStateChange onStateChange) {
        onStateChange.accept(new RequestEvent.Processing());
        LocalServiceResponse response = new LocalServiceResponse(onStateChange);
        try {
            serve(request, response);
            response.close();
        } catch (Exception e) {
            onStateChange.accept(new RequestEvent.Failed(e.getMessage()));
        }
    }
    
    /**
     * Returns the service name, derived from the concrete class name.
     */
    public final String serviceName() {
        return this.getClass().getSimpleName();
    }

    void setManager(ServiceManager manager) {
        this.manager = manager;
    }
}
