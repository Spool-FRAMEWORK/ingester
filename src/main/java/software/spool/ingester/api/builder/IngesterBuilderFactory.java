package software.spool.ingester.api.builder;

import software.spool.core.adapter.watchdog.HttpWatchdogClient;
import software.spool.core.model.watchdog.ModuleIdentity;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingHeartbeat;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.Objects;

public class IngesterBuilderFactory {
    public static IngesterBuilder reactive() {
        return new Configuration().reactive();
    }

    public static IngesterBuilder buffered() {
        return new Configuration().buffered();
    }

    public static Configuration watchdog(String url, String moduleId) {
        return new Configuration(url, moduleId);
    }

    public static final class Configuration {
        private final String watchdogUrl;
        private final String moduleId;

        private Configuration(String watchdogUrl, String moduleId) {
            this.watchdogUrl = watchdogUrl;
            this.moduleId = moduleId;
        }

        private Configuration() {
            this(null, "ingester");
        }

        public IngesterBuilder reactive() {
            return new IngesterBuilder(buildHeartbeat(watchdogUrl, moduleId)).flushPolicy(FlushPolicy.immediate());
        }

        public IngesterBuilder buffered() {
            return new IngesterBuilder(buildHeartbeat(watchdogUrl, moduleId)).flushPolicy(FlushPolicy.whenReaches(100).orEvery(Duration.ofSeconds(60)));
        }
    }

    private static ModuleHeartBeat buildHeartbeat(String watchdogUrl, String moduleId) {
        return Objects.isNull(watchdogUrl) ?
                ModuleHeartBeat.NOOP : new PollingHeartbeat(
                new HttpWatchdogClient(watchdogUrl),
                ModuleIdentity.of(moduleId)
        );
    }
}
