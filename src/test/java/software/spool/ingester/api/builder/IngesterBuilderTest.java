package software.spool.ingester.api.builder;

import org.junit.jupiter.api.Test;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.port.DataLakeWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class IngesterBuilderTest {

    @Test
    void create_withoutQuarantine_buildsTheIngester() {
        Ingester ingester = builder().create();

        assertThat(ingester).isNotNull();
    }

    @Test
    void create_withoutInboxReader_stillFailsWithItsOwnMessage() {
        IngesterBuilder builder = IngesterBuilderFactory.reactive()
                .from(mock(EventSubscriber.class))
                .storesWith(mock(DataLakeWriter.class))
                .on(mock(EventPublisher.class));

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
