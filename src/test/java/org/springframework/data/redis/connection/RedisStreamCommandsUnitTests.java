package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;

/**
 * Unit tests for {@link RedisStreamCommands}.
 *
 * @author jinkshower
 */
class RedisStreamCommandsUnitTests {

	@Test // GH-2982
	void xPendingOptionsUnboundedShouldThrowExceptionWhenCountIsNegative() {

		assertThatThrownBy(() -> XPendingOptions.unbounded(-1L)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-2982
	void xPendingOptionsRangeShouldThrowExceptionWhenRangeIsNull() {

		assertThatThrownBy(() -> XPendingOptions.range(null, 10L)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-2982
	void xPendingOptionsRangeShouldThrowExceptionWhenCountIsNegative() {

		Range<?> range = Range.closed("0", "10");

		assertThatThrownBy(() -> XPendingOptions.range(range, -1L)).isInstanceOf(IllegalArgumentException.class);
	}
}
