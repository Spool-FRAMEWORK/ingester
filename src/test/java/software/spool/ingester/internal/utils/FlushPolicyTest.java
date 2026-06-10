package software.spool.ingester.internal.utils;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class FlushPolicyTest {

    @Test
    void shouldFlush_emptyBuffer_returnsFalse() {
        assertThat(FlushPolicy.immediate().shouldFlush(0, Duration.ZERO)).isFalse();
    }

    @Test
    void shouldFlush_belowSizeThreshold_returnsFalse() {
        assertThat(FlushPolicy.whenReaches(5).shouldFlush(3, Duration.ZERO)).isFalse();
    }

    @Test
    void shouldFlush_atSizeThreshold_returnsTrue() {
        assertThat(FlushPolicy.whenReaches(5).shouldFlush(5, Duration.ZERO)).isTrue();
    }

    @Test
    void shouldFlush_belowTimeThreshold_returnsFalse() {
        assertThat(FlushPolicy.every(Duration.ofSeconds(5)).shouldFlush(1, Duration.ofSeconds(2))).isFalse();
    }

    @Test
    void shouldFlush_atTimeThreshold_returnsTrue() {
        assertThat(FlushPolicy.every(Duration.ofSeconds(5)).shouldFlush(1, Duration.ofSeconds(5))).isTrue();
    }
}
