package software.spool.ingester.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.port.health.HealthStatus;
import software.spool.core.model.Event;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.bus.Subscription;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.internal.utils.Buffer;
import software.spool.ingester.internal.utils.FlushCoordinator;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class IngesterTest {

    private SpoolNode.StartPermit permit;

    @BeforeEach
    void setUp() {
        permit = mock(SpoolNode.StartPermit.class);
    }

    @Test
    void start_delegatesToSubscriber() {
        AtomicInteger subscribeCount = new AtomicInteger();
        Ingester ingester = new Ingester(
            new EventSubscriber() {
                @Override public <E extends Event> Subscription subscribe(Class<E> eventType, Handler<E> handler) {
                    subscribeCount.incrementAndGet();
                    return Subscription.NULL;
                }
            },
            polling(), coordinator(), ModuleHeartBeat.NOOP, new ErrorRouter()
        );

        ingester.start(permit);

        assertThat(subscribeCount.get()).isEqualTo(1);
    }

    @Test
    void start_idempotent_doesNotStartTwice() {
        AtomicInteger subscribeCount = new AtomicInteger();
        Ingester ingester = new Ingester(
            new EventSubscriber() {
                @Override public <E extends Event> Subscription subscribe(Class<E> eventType, Handler<E> handler) {
                    subscribeCount.incrementAndGet();
                    return Subscription.NULL;
                }
            },
            polling(), coordinator(), ModuleHeartBeat.NOOP, new ErrorRouter()
        );

        ingester.start(permit);
        ingester.start(permit);

        assertThat(subscribeCount.get()).isEqualTo(1);
    }

    @Test
    void checkHealth_whenStopped_returnsDegraded() {
        Ingester ingester = new Ingester(
            new EventSubscriber() {
                @Override public <E extends Event> Subscription subscribe(Class<E> t, Handler<E> h) { return Subscription.NULL; }
            },
            polling(), coordinator(), ModuleHeartBeat.NOOP, new ErrorRouter()
        );

        assertThat(ingester.checkHealth().status()).isEqualTo(HealthStatus.DEGRADED);
    }

    @Test
    void checkHealth_whenRunning_returnsNotDegraded() {
        Ingester ingester = new Ingester(
            new EventSubscriber() {
                @Override public <E extends Event> Subscription subscribe(Class<E> t, Handler<E> h) { return Subscription.NULL; }
            },
            polling(), coordinator(), ModuleHeartBeat.NOOP, new ErrorRouter()
        );

        ingester.start(permit);

        assertThat(ingester.checkHealth().status()).isNotEqualTo(HealthStatus.DEGRADED);
    }

    @Test
    void stop_afterStart_healthBecomesDegraded() {
        Ingester ingester = new Ingester(
            new EventSubscriber() {
                @Override public <E extends Event> Subscription subscribe(Class<E> t, Handler<E> h) { return Subscription.NULL; }
            },
            polling(), coordinator(), ModuleHeartBeat.NOOP, new ErrorRouter()
        );

        ingester.start(permit);
        ingester.stop(permit);

        assertThat(ingester.checkHealth().status()).isEqualTo(HealthStatus.DEGRADED);
    }

    private static PollingConfiguration polling() {
        return new PollingConfiguration((task, policy, token) -> {}, PollingPolicy.ONCE);
    }

    private static FlushCoordinator coordinator() {
        return new FlushCoordinator(new Buffer(), FlushPolicy.whenReaches(100), items -> {});
    }
}
