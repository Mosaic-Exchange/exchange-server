package org.rumor.service;

@FunctionalInterface
public interface OnReceive {
    void accept(byte[] data);
}
