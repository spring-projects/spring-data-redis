/*
 * Copyright 2018-2020 the original author or authors.
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

import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for {@link DefaultBoundValueOperations}
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultBoundValueOperationsUnitTests {

	private DefaultBoundValueOperations<String, Object> boundValueOps;

	@Mock ValueOperations<String, Object> valueOps;

	private static final String KEY = "key-1";
	private static final Object VALUE = "value";

	@BeforeEach
	void setUp() {

		RedisOperations<String, Object> redisOps = mock(RedisOperations.class);
		when(redisOps.opsForValue()).thenReturn(valueOps);

		boundValueOps = new DefaultBoundValueOperations<>(KEY, redisOps);
	}

	@Test // DATAREDIS-786
	void setIfPresentShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE);

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE));
	}

	@Test // DATAREDIS-786
	void setIfPresentWithTimeoutShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE, 10, TimeUnit.SECONDS);

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE), eq(10L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	void setWithDurationOfSecondsShouldDelegateCorrectly() {

		boundValueOps.set(VALUE, Duration.ofSeconds(1));

		verify(valueOps).set(eq(KEY), eq(VALUE), eq(1L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	void setWithDurationOfMillisShouldDelegateCorrectly() {

		boundValueOps.set(VALUE, Duration.ofMillis(250));

		verify(valueOps).set(eq(KEY), eq(VALUE), eq(250L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAREDIS-815
	void setIfAbsentWithDurationOfSecondsShouldDelegateCorrectly() {

		boundValueOps.setIfAbsent(VALUE, Duration.ofSeconds(1));

		verify(valueOps).setIfAbsent(eq(KEY), eq(VALUE), eq(1L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	void setIfAbsentWithDurationOfMillisShouldDelegateCorrectly() {

		boundValueOps.setIfAbsent(VALUE, Duration.ofMillis(250));

		verify(valueOps).setIfAbsent(eq(KEY), eq(VALUE), eq(250L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAREDIS-815
	void setIfPresentWithDurationOfSecondsShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE, Duration.ofSeconds(1));

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE), eq(1L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	void setIfPresentWithDurationOfMillisShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE, Duration.ofMillis(250));

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE), eq(250L), eq(TimeUnit.MILLISECONDS));
	}
}
