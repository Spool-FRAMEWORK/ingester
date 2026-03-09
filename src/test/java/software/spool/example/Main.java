package software.spool.example;

import software.spool.core.adapter.InMemoryEventBus;
import software.spool.core.adapter.InMemoryInbox;
import software.spool.core.model.IdempotencyKey;
import software.spool.core.model.ItemPublished;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.builder.Ingesters;
import software.spool.ingester.internal.adapter.*;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.stream.IntStream;

public class Main {
    private static final InMemoryEventBus bus = createEventBus();
    private static final InMemoryDataLake dataLake = createDataLake();

    public static void main(String[] args) throws InterruptedException {
        Ingester ingester = Ingesters.buffered()
                .from(bus)
                .storesWith(dataLake)
                .updatedWith(new InMemoryInbox())
                .on(bus)
                .flushWith(FlushPolicy.every(Duration.ofSeconds(3)))
                .build();
        ingester.startIngestion();
        emitValues();
    }

    private static void emitValues() {
        IntStream.range(1, 100).mapToObj(
                 n -> ItemPublished.builder()
                         .idempotencyKey(IdempotencyKey.of("mock", Integer.toString(n)))
                         .payload(Integer.toString(n))
                         .build()
         ).forEach(bus::emit);
    }

    private static InMemoryEventBus createEventBus() {
        return new InMemoryEventBus();
    }

    private static InMemoryDataLake createDataLake() {
        return new InMemoryDataLake();
    }
}
