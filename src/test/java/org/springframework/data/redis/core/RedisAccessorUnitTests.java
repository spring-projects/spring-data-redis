/*
 * Copyright 2017-2025 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * Unit tests for {@link RedisAccessor}.
 *
 * @author John Blum
 */
class RedisAccessorUnitTests {

	@Test
	void setAndGetConnectionFactory() {

		RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);

		RedisAccessor redisAccessor = new TestRedisAccessor();

		assertThat(redisAccessor.getConnectionFactory()).isNull();

		redisAccessor.setConnectionFactory(mockConnectionFactory);

		assertThat(redisAccessor.getConnectionFactory()).isSameAs(mockConnectionFactory);
		assertThat(redisAccessor.getRequiredConnectionFactory()).isSameAs(mockConnectionFactory);

		redisAccessor.setConnectionFactory(null);

		assertThat(redisAccessor.getConnectionFactory()).isNull();

		verifyNoInteractions(mockConnectionFactory);
	}

	@Test
	void getRequiredConnectionFactoryWhenNull() {

		assertThatIllegalStateException().isThrownBy(() -> new TestRedisAccessor().getRequiredConnectionFactory())
				.withMessage("RedisConnectionFactory is required").withNoCause();
	}

	@Test
	void afterPropertiesSetCallsGetRequiredConnectionFactory() {

		RedisConnectionFactory mockConnectionFactory = mock(RedisConnectionFactory.class);

		RedisAccessor redisAccessor = spy(new TestRedisAccessor());

		doReturn(mockConnectionFactory).when(redisAccessor).getRequiredConnectionFactory();

		redisAccessor.afterPropertiesSet();

		verify(redisAccessor, times(1)).afterPropertiesSet();
		verify(redisAccessor, times(1)).getRequiredConnectionFactory();
		verifyNoMoreInteractions(redisAccessor);
	}

	static class TestRedisAccessor extends RedisAccessor {}

}
