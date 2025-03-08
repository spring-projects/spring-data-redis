package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.params.XPendingParams;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;

/**
 * @author Jeonggyu Choi
 */
class StreamConvertersUnitTest {

	@Test // GH-2046
	void shouldConvertIdle() throws NoSuchFieldException, IllegalAccessException {
		XPendingOptions options = XPendingOptions.unbounded(5L).idle(Duration.of(1, ChronoUnit.HOURS));

		XPendingParams xPendingParams = StreamConverters.toXPendingParams(options);

		Field idle = XPendingParams.class.getDeclaredField("idle");
		idle.setAccessible(true);
		Long idleValue = (Long) idle.get(xPendingParams);
		assertThat(idleValue).isEqualTo(Duration.of(1, ChronoUnit.HOURS).toMillis());
	}
}
