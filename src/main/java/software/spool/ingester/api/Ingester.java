package software.spool.ingester.api;

import software.spool.core.control.Handler;
import software.spool.core.model.ItemPublished;
import software.spool.core.port.EventBusListener;
import software.spool.core.port.Subscription;
import software.spool.ingester.internal.utils.Buffer;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Ingester {
    private final EventBusListener listener;
    private final Handler<Collection<ItemPublished>> handler;
    private final Buffer buffer;
    private Subscription subscription;
    private ScheduledExecutorService scheduler;

    public Ingester(EventBusListener listener, Handler<Collection<ItemPublished>> handler, Buffer buffer) {
        this.listener = listener;
        this.handler = handler;
        this.buffer = buffer;
        this.subscription = Subscription.NULL;
    }

    public void startIngestion() {
        if (subscription != Subscription.NULL) return;
        subscription = listener.on(ItemPublished.class, i -> {
            buffer.insert(i);
            if (buffer.shouldFlush()) handler.handle(buffer.flush());
        });
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            if (buffer.shouldFlush()) handler.handle(buffer.flush());
        }, 200, 200, TimeUnit.MILLISECONDS);
    }

    public void stopIngestion() {
        if (subscription == Subscription.NULL) return;
        subscription.cancel();
        subscription = Subscription.NULL;
        scheduler.shutdown();
    }
}
