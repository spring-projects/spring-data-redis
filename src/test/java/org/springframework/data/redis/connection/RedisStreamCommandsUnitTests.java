/*
 * Copyright 2024-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;

/**
 * Unit tests for {@link RedisStreamCommands}.
 *
 * @author jinkshower
 * @author Jeonggyu Choi
 */
class RedisStreamCommandsUnitTests {

	@Test // GH-2982
	void xPendingOptionsUnboundedShouldThrowExceptionWhenCountIsNegative() {
		assertThatIllegalArgumentException().isThrownBy(() -> XPendingOptions.unbounded(-1L));
	}

	@Test // GH-2982
	void xPendingOptionsRangeShouldThrowExceptionWhenRangeIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> XPendingOptions.range(null, 10L));
	}

	@Test // GH-2982
	void xPendingOptionsRangeShouldThrowExceptionWhenCountIsNegative() {

		Range<?> range = Range.closed("0", "10");

		assertThatIllegalArgumentException().isThrownBy(() -> XPendingOptions.range(range, -1L));
	}

	@Test // GH-2046
	void xPendingOptionsIdleShouldThrowExceptionWhenIdleIsNull() {
		XPendingOptions xPendingOptions = XPendingOptions.unbounded();

		assertThatIllegalArgumentException().isThrownBy(() -> xPendingOptions.idle(null));
	}
}
