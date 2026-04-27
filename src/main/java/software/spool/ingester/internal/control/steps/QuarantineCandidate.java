package software.spool.ingester.internal.control.steps;

import software.spool.ingester.internal.model.EnvelopeStoredContext;

import java.util.List;

record QuarantineCandidate(EnvelopeStoredContext context, List<String> violations) {}