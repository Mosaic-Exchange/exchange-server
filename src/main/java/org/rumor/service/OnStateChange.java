package org.rumor.service;

@FunctionalInterface
public interface OnStateChange {
    void accept(RequestState state);
}
