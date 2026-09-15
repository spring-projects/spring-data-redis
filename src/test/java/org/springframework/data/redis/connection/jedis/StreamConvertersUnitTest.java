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

import static org.assertj.core.api.Assertions.*;

import redis.clients.jedis.params.XPendingParams;

import java.time.Duration;

import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisStreamCommands.XPendingOptions;

import static org.springframework.data.redis.test.util.IntRangeAssertions.*;

import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;

import org.springframework.data.redis.connection.stream.StreamReadOptions;

/**
 * @author Jeonggyu Choi
 * @author Christoph Strobl
 * @author Viktoriya Kutsarova
 * @author Moritz Halbritter
 */
class StreamConvertersUnitTest {

	@Test // GH-2046
	void shouldConvertIdle() {

		XPendingOptions options = XPendingOptions.unbounded(5L).minIdleTime(Duration.of(1, ChronoUnit.HOURS));

		XPendingParams xPendingParams = StreamConverters.toXPendingParams(options);

		assertThat(xPendingParams).hasFieldOrPropertyWithValue("idle", Duration.of(1, ChronoUnit.HOURS).toMillis());
	}

	@Test // GH-3436
	void toXPendingParamsShouldRejectCountOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Count for xPending in Jedis",
				(count) -> StreamConverters.toXPendingParams(XPendingOptions.unbounded(count)));
	}

	@Test // GH-3436
	void toXClaimParamsShouldRejectRetryCountOutsideIntegerRange() {
		assertRejectsOutOfIntRange("RetryCount for xClaim in Jedis",
				(retryCount) -> StreamConverters.toXClaimParams(XClaimOptions.minIdleMs(0).ids("1-1").retryCount(retryCount)));
	}

	@Test // GH-3436
	void toXReadParamsShouldRejectBlockOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Block for xRead in Jedis",
				(block) -> StreamConverters.toXReadParams(readWithBlock(block)));
	}

	@Test // GH-3436
	void toXReadParamsShouldRejectCountOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Count for xRead in Jedis",
				(count) -> StreamConverters.toXReadParams(StreamReadOptions.empty().count(count)));
	}

	@Test // GH-3436
	void toXReadGroupParamsShouldRejectBlockOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Block for xReadGroup in Jedis",
				(block) -> StreamConverters.toXReadGroupParams(readWithBlock(block)));
	}

	@Test // GH-3436
	void toXReadGroupParamsShouldRejectCountOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Count for xReadGroup in Jedis",
				(count) -> StreamConverters.toXReadGroupParams(StreamReadOptions.empty().count(count)));
	}

	// Duration.ofMillis is the only way to set a block timeout, so out-of-range values need the round trip
	private static StreamReadOptions readWithBlock(long blockMillis) {
		return StreamReadOptions.empty().block(Duration.ofMillis(blockMillis));
	}
}
