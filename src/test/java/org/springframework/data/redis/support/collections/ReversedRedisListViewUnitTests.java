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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link ReversedRedisListView}.
 *
 * @author Jialin Chen
 */
@ExtendWith(MockitoExtension.class)
class ReversedRedisListViewUnitTests {

	@Mock RedisList<String> listMock;

	private RedisList<String> reversed;

	@BeforeEach
	void before() {
		reversed = new ReversedRedisListView<>(listMock);
	}

	@Test // GH-3458
	void peekShouldReturnLastElementOfList() {

		when(listMock.peekLast()).thenReturn("last");

		assertThat(reversed.peek()).isEqualTo("last");
	}

	@Test // GH-3458
	void pollShouldRemoveLastElementOfList() {

		when(listMock.pollLast()).thenReturn("last");

		assertThat(reversed.poll()).isEqualTo("last");
	}

	@Test // GH-3458
	void pollWithTimeoutShouldRemoveLastElementOfListUsingTimeout() throws InterruptedException {

		when(listMock.pollLast(5, TimeUnit.SECONDS)).thenReturn("last");

		assertThat(reversed.poll(5, TimeUnit.SECONDS)).isEqualTo("last");
	}
}
