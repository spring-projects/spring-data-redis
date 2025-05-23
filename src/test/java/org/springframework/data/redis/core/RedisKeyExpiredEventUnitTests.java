/*
 * Copyright 2018-2025 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;


/**
 * Unit tests for {@link RedisKeyExpiredEvent}.
 *
 * @author Mark Paluch
 */
class RedisKeyExpiredEventUnitTests {

	@Test // DATAREDIS-744
	void shouldReturnKeyspace() {

		assertThat(new RedisKeyExpiredEvent<>("foo".getBytes(), "").getKeyspace()).isNull();
		assertThat(new RedisKeyExpiredEvent<>("foo:bar".getBytes(), "").getKeyspace()).isEqualTo("foo");
		assertThat(new RedisKeyExpiredEvent<>("foo:bar:baz".getBytes(), "").getKeyspace()).isEqualTo("foo");
	}

	@Test // DATAREDIS-744
	void shouldReturnId() {

		assertThat(new RedisKeyExpiredEvent<>("foo".getBytes(), "").getId()).isEqualTo("foo".getBytes());
		assertThat(new RedisKeyExpiredEvent<>("foo:bar".getBytes(), "").getId()).isEqualTo("bar".getBytes());
		assertThat(new RedisKeyExpiredEvent<>("foo:bar:baz".getBytes(), "").getId()).isEqualTo("bar:baz".getBytes());
	}
}
