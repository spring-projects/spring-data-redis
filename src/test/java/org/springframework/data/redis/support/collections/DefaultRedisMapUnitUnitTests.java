/*
 * Copyright 2018-2024 the original author or authors.
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

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.redis.core.BoundHashOperations;

/**
 * Unit tests for {@link DefaultRedisMap}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class DefaultRedisMapUnitUnitTests {

	@Mock BoundHashOperations<String, String, String> operationsMock;

	private DefaultRedisMap<String, String> map;

	@BeforeEach
	void before() {
		map = new DefaultRedisMap<>(operationsMock);
	}

	@Test // DATAREDIS-803
	void shouldGetEntrySet() {

		when(operationsMock.entries()).thenReturn(Collections.singletonMap("foo", "bar"));

		Set<Entry<String, String>> result = map.entrySet();

		assertThat(result).hasSize(1);
	}
}
