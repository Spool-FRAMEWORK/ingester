package software.spool.ingester.internal.utils;

import org.junit.jupiter.api.Test;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.IdempotencyKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FlushCoordinatorTest {

    @Test
    void submit_policyDoesNotTriggerFlush_handlerNotCalled() {
        List<Collection<EnvelopeStored>> flushed = new ArrayList<>();
        FlushCoordinator coordinator = new FlushCoordinator(new Buffer(), FlushPolicy.whenReaches(100), flushed::add);

        coordinator.submit(anyStored());

        assertThat(flushed).isEmpty();
    }

    @Test
    void submit_policyTriggersFlush_handlerCalledWithItem() {
        List<Collection<EnvelopeStored>> flushed = new ArrayList<>();
        FlushCoordinator coordinator = new FlushCoordinator(new Buffer(), FlushPolicy.immediate(), flushed::add);

        coordinator.submit(anyStored());

        assertThat(flushed).hasSize(1);
        assertThat(flushed.get(0)).hasSize(1);
    }

    @Test
    void flushIfNeeded_emptyBuffer_handlerNotCalled() {
        List<Collection<EnvelopeStored>> flushed = new ArrayList<>();
        FlushCoordinator coordinator = new FlushCoordinator(new Buffer(), FlushPolicy.immediate(), flushed::add);

        coordinator.flushIfNeeded();

        assertThat(flushed).isEmpty();
    }

    private static EnvelopeStored anyStored() {
        return EnvelopeStored.builder().idempotencyKey(IdempotencyKey.of("k")).build();
    }
}
