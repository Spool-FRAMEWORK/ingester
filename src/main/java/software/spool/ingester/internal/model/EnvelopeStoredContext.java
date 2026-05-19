package software.spool.ingester.internal.model;

import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.IdempotencyKey;

public record EnvelopeStoredContext(
    EnvelopeStored event,
    Envelope envelope
) {
    public IdempotencyKey idempotencyKey() {
        return event.idempotencyKey();
    }

    public byte[] payload() {
        return envelope.payload();
    }
}