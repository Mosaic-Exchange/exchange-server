package org.rumor.service;

/**
 * Base class for all services in the Rumor framework.
 *
 * Extend this class and implement {@link #serve(byte[], ServiceResponse)} to handle
 * incoming requests from remote peers. Use {@link #request(byte[], OnReceive, OnStateChange)}
 * to invoke this service on a remote peer that offers it.
 *
 * The service is identified by the simple class name of the subclass.
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
     * Send a request to a remote peer offering this service.
     * The framework picks a suitable live node automatically.
     *
     * @param request       request bytes sent to the remote peer
     * @param onReceive     called with each chunk of response data as it arrives
     * @param onStateChange called when the request state changes (PROCESSING, SUCCEEDED, FAILED)
     */
    public void request(byte[] request, OnReceive onReceive, OnStateChange onStateChange) {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        manager.sendRequest(serviceName(), request, onReceive, onStateChange);
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
