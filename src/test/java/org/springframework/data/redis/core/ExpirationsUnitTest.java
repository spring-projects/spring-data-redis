/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.springframework.data.redis.core.Expirations.Timeouts;

/**
 * @author Christoph Strobl
 * @since 2025/02
 */
class ExpirationsUnitTest {

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String KEY_3 = "key-3";

	@ParameterizedTest
	@EnumSource(TimeUnit.class)
	void expirationMemorizesSourceUnit(TimeUnit targetUnit) {

		Expirations<String> exp = Expirations.of(targetUnit, List.of(KEY_1), new Timeouts(TimeUnit.SECONDS, List.of(120L)));

		assertThat(exp.ttl().get(0)).satisfies(expiration -> {
			assertThat(expiration.raw()).isEqualTo(120L);
			assertThat(expiration.value()).isEqualTo(targetUnit.convert(120, TimeUnit.SECONDS));
		});
	}

	@Test
	void expirationsCategorizesElements() {

		Expirations<String> exp = createExpirations(new Timeouts(TimeUnit.SECONDS, List.of(-2L, -1L, 120L)));

		assertThat(exp.persistent()).containsExactly(KEY_2);
		assertThat(exp.missing()).containsExactly(KEY_1);
		assertThat(exp.expiring()).containsExactly(Map.entry(KEY_3, Duration.ofMinutes(2)));
	}

	@Test
	void returnsNullForMissingElements() {

		Expirations<String> exp = createExpirations(new Timeouts(TimeUnit.SECONDS, List.of(-2L, -1L, 120L)));

		assertThat(exp.expirationOf("missing")).isNull();
		assertThat(exp.ttlOf("missing")).isNull();
	}

	@Test
	void ttlReturnsDurationForEntriesWithTimeout() {

		Expirations<String> exp = createExpirations(new Timeouts(TimeUnit.SECONDS, List.of(-2L, -1L, 120L)));

		assertThat(exp.ttlOf(KEY_3)).isEqualTo(Duration.ofMinutes(2));
	}

	@Test
	void ttlReturnsNullForPersistentAndMissingEntries() {

		Expirations<String> exp = createExpirations(new Timeouts(TimeUnit.SECONDS, List.of(-2L, -1L, 120L)));

		assertThat(exp.ttlOf(KEY_1)).isNull();
		assertThat(exp.ttlOf(KEY_2)).isNull();
	}

	static Expirations<String> createExpirations(Timeouts timeouts) {

		List<String> keys = IntStream.range(1, timeouts.raw().size() + 1).mapToObj("key-%s"::formatted).toList();
		return Expirations.of(timeouts.timeUnit(), keys, timeouts);
	}
}
