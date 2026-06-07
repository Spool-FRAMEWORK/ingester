package software.spool.ingester.api.port;

import java.time.Instant;
import java.util.List;

public record QuarantinedRecord(
        byte[] original,
        List<String> violations,
        Instant quarantinedAt
) {}
