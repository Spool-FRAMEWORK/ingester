package software.spool.ingester.api.port;

import java.time.Instant;
import java.util.List;

public record QuarantinedRecord(
        String original,
        List<String> violations,
        Instant quarantinedAt
) {}
