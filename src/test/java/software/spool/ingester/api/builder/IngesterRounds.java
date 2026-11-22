package software.spool.ingester.api.builder;

import software.spool.core.model.Event;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.bus.Subscription;
import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.ingester.fixture.MovableClock;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

/**
 * An ingester built by its builder whose flush rounds run when a test asks, with a clock the test moves. It is here
 * because the polling and the clock of the builder are only reachable from its package.
 */
public final class IngesterRounds {
    private static final Duration EVERY = Duration.ofMinutes(1);
    private final MovableClock clock;
    private final DirectSubscriber bus = new DirectSubscriber();
    private final ManualScheduler scheduler = new ManualScheduler();
    private final List<PartitionedEnvelope> written = new ArrayList<>();
    private final List<Event> published = new ArrayList<>();

    public IngesterRounds(InboxEnvelopeResolver inbox, ProcessedKeyRepository processedKeys, MovableClock clock) {
        this.clock = clock;
        IngesterBuilderFactory.buffered()
                .flushPolicy(FlushPolicy.whenReaches(Integer.MAX_VALUE).orEvery(EVERY))
                .from(bus)
                .readWith(inbox)
                .storesWith(this::write)
                .on(new EventPublisher() {
                    @Override public <E extends Event> void publish(E event) {
                        published.add(event);
                    }
                })
                .rememberProcessedKeysIn(processedKeys)
                .clock(clock)
                .polling(new PollingConfiguration(scheduler, Schedule.every(EVERY)))
                .create()
                .start(mock(SpoolNode.StartPermit.class));
    }

    public void announce(EnvelopeStored event) {
        bus.emit(event);
    }

    public void writeBatch() {
        clock.advance(EVERY);
        scheduler.runRound();
    }

    public List<PartitionedEnvelope> written() {
        return List.copyOf(written);
    }

    public List<Event> published() {
        return List.copyOf(published);
    }

    private Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> items) {
        written.addAll(items);
        return items.stream().map(item -> item.envelope().idempotencyKey());
    }

    private static final class ManualScheduler implements PollingScheduler {
        private Runnable task;

        @Override
        public void schedule(Runnable task, Schedule schedule, CancellationToken token) {
            this.task = task;
        }

        void runRound() {
            task.run();
        }
    }

    private static final class DirectSubscriber implements EventSubscriber {
        private final Map<Class<?>, Handler<?>> handlers = new HashMap<>();

        @Override
        public <E extends Event> Subscription subscribe(Class<E> eventType, Handler<E> handler) {
            handlers.put(eventType, handler);
            return Subscription.NULL;
        }

        @SuppressWarnings("unchecked")
        <E extends Event> void emit(E event) {
            ((Handler<E>) handlers.get(event.getClass())).handle(event);
        }
    }
}
