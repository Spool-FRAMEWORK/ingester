package software.spool.ingester.api.port;

import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.PartitionKey;

import java.util.Objects;

public record PartitionedEnvelope(PartitionKey partition, Envelope envelope) {
    public PartitionedEnvelope {
        Objects.requireNonNull(partition);
        Objects.requireNonNull(envelope);
    }
}
