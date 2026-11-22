package software.spool.ingester.internal.decorator;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.core.port.idempotency.ProcessedKeyRepository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Does not write again the envelopes whose key is remembered, but reports them as written, so the ingester announces
 * them again without rewriting them, and remembers the keys the delegate wrote (ADR-007). Envelopes sent to quarantine
 * never reach the writer, so they are never remembered (ADR-010).
 */
public class RememberingDataLakeWriter implements DataLakeWriter {
    private final DataLakeWriter writer;
    private final ProcessedKeyRepository processedKeys;

    public RememberingDataLakeWriter(DataLakeWriter writer, ProcessedKeyRepository processedKeys) {
        this.writer = writer;
        this.processedKeys = processedKeys;
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> items) {
        Map<Boolean, List<PartitionedEnvelope>> byRemembered = items.stream()
                .collect(Collectors.partitioningBy(item -> processedKeys.contains(keyOf(item))));
        List<IdempotencyKey> written = byRemembered.get(false).isEmpty()
                ? List.of()
                : writer.write(byRemembered.get(false)).toList();
        written.forEach(processedKeys::save);
        return Stream.concat(byRemembered.get(true).stream().map(RememberingDataLakeWriter::keyOf), written.stream());
    }

    private static IdempotencyKey keyOf(PartitionedEnvelope item) {
        return item.envelope().idempotencyKey();
    }
}
