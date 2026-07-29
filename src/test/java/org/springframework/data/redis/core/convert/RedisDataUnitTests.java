/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.core.convert;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RedisData}.
 */
class RedisDataUnitTests {

	@Test
	void setTimeToLiveWithTimeUnitConvertsToSeconds() {

		RedisData redisData = new RedisData(Collections.emptyMap());

		redisData.setTimeToLive(2L, TimeUnit.MINUTES);

		assertThat(redisData.getTimeToLive()).isEqualTo(120L);
	}

	@Test
	void setTimeToLiveWithNullTimeToLiveThrowsIllegalArgumentException() {

		RedisData redisData = new RedisData(Collections.emptyMap());

		assertThatIllegalArgumentException().isThrownBy(() -> redisData.setTimeToLive(null, TimeUnit.SECONDS))
				.withMessageContaining("TTL");
	}

	@Test
	void setTimeToLiveWithNullTimeUnitThrowsIllegalArgumentException() {

		RedisData redisData = new RedisData(Collections.emptyMap());

		assertThatIllegalArgumentException().isThrownBy(() -> redisData.setTimeToLive(1L, null))
				.withMessageContaining("TimeUnit");
	}

}
