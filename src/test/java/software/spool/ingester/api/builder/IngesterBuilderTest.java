package software.spool.ingester.api.builder;

import org.junit.jupiter.api.Test;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class IngesterBuilderTest {

    @Test
    void create_withoutQuarantineStore_failsWithAClearMessage() {
        IngesterBuilder builder = builder();

        assertThatThrownBy(builder::create)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("QuarantineStore required");
    }

    @Test
    void create_withQuarantineStore_buildsTheIngester() {
        Ingester ingester = builder().quarantineStore(mock(QuarantineStore.class)).create();

        assertThat(ingester).isNotNull();
    }

    @Test
    void create_withoutInboxReader_stillFailsWithItsOwnMessage() {
        IngesterBuilder builder = IngesterBuilderFactory.reactive()
                .from(mock(EventSubscriber.class))
                .storesWith(mock(DataLakeWriter.class))
                .on(mock(EventPublisher.class))
                .quarantineStore(mock(QuarantineStore.class));

        assertThatThrownBy(builder::create)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("InboxReader required");
    }

    private static IngesterBuilder builder() {
        return IngesterBuilderFactory.reactive()
                .from(mock(EventSubscriber.class))
                .storesWith(mock(DataLakeWriter.class))
                .readWith(mock(InboxEnvelopeResolver.class))
                .on(mock(EventPublisher.class));
    }
}
