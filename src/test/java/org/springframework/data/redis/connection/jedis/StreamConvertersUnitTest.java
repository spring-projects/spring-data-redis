/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.assertThat;

import redis.clients.jedis.params.XPendingParams;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;

/**
 * @author Jeonggyu Choi
 * @author Christoph Strobl
 */
class StreamConvertersUnitTest {

	@Test // GH-2046
	void shouldConvertIdle() {

		XPendingOptions options = XPendingOptions.unbounded(5L).minIdleTime(Duration.of(1, ChronoUnit.HOURS));

		XPendingParams xPendingParams = StreamConverters.toXPendingParams(options);

		assertThat(xPendingParams).hasFieldOrPropertyWithValue("idle", Duration.of(1, ChronoUnit.HOURS).toMillis());
	}
}
