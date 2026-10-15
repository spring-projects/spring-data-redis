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
package org.springframework.data.redis.support.collections;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisOperations;

/**
 * Unit tests for {@link DefaultRedisList}.
 *
 * @author JungKyun Ryu
 */
@ExtendWith(MockitoExtension.class)
class DefaultRedisListUnitTests {

	@Mock BoundListOperations<String, String> listOperations;
	@Mock RedisOperations<String, String> redisOperations;

	private DefaultRedisList<String> list;

	@BeforeEach
	void setUp() {

		when(listOperations.getKey()).thenReturn("list");
		when(listOperations.getOperations()).thenReturn(redisOperations);
		list = new DefaultRedisList<>(listOperations);
	}

	@Test
	void getFromEmptyListShouldThrowIndexOutOfBoundsException() {

		when(listOperations.size()).thenReturn(0L);

		assertThatIndexOutOfBoundsException().isThrownBy(() -> list.get(0));
	}

	@Test
	void getAtSizeShouldThrowIndexOutOfBoundsException() {

		when(listOperations.size()).thenReturn(3L);

		assertThatIndexOutOfBoundsException().isThrownBy(() -> list.get(3));
	}

	@Test
	void getLastElementShouldReturnValue() {

		when(listOperations.size()).thenReturn(3L);
		when(listOperations.index(2)).thenReturn("C");

		assertThat(list.get(2)).isEqualTo("C");
	}
}
