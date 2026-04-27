package software.spool.ingester.internal.utils;

import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.port.bus.Handler;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

public class FlushCoordinator {
    private final Buffer buffer;
    private final FlushPolicy policy;
    private final Handler<Collection<EnvelopeStored>> handler;
    private Instant lastFlush = Instant.now();

    public FlushCoordinator(Buffer buffer, FlushPolicy policy,
            Handler<Collection<EnvelopeStored>> handler) {
        this.buffer = buffer;
        this.policy = policy;
        this.handler = handler;
    }

    public void submit(EnvelopeStored envelopeStored) {
        buffer.add(envelopeStored);
        flushIfNeeded();
    }

    public void flushIfNeeded() {
        if (!policy.shouldFlush(buffer.size(), Duration.between(lastFlush, Instant.now()))) return;
        Collection<EnvelopeStored> envelopeStoreEvents = buffer.drain();
        if (envelopeStoreEvents.isEmpty()) return;
        lastFlush = Instant.now();
        handler.handle(envelopeStoreEvents);
    }
}
