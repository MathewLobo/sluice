package io.github.mathewlobo.sluice.server;

import java.io.IOException;

/** Thrown by LogStore.append when the queue is full: the server is overloaded, try again later. */
public class StoreBusyException extends IOException {
    public StoreBusyException(String message) {
        super(message);
    }
}