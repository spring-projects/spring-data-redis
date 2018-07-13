/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for {@link DefaultBoundValueOperations}
 * 
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBoundValueOperationsUnitTests {

	DefaultBoundValueOperations<String, Object> boundValueOps;

	@Mock ValueOperations<String, Object> valueOps;

	static final String KEY = "key-1";
	static final Object VALUE = "value";

	@Before
	public void setUp() {

		RedisOperations<String, Object> redisOps = mock(RedisOperations.class);
		when(redisOps.opsForValue()).thenReturn(valueOps);

		boundValueOps = new DefaultBoundValueOperations<>(KEY, redisOps);
	}

	@Test // DATAREDIS-786
	public void setIfPresentShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE);

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE));
	}

	@Test // DATAREDIS-786
	public void setIfPresentWithTimeoutShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE, 10, TimeUnit.SECONDS);

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE), eq(10L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	public void setWithDurationOfSecondsShouldDelegateCorrectly() {

		boundValueOps.set(VALUE, Duration.ofSeconds(1));

		verify(valueOps).set(eq(KEY), eq(VALUE), eq(1L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	public void setWithDurationOfMillisShouldDelegateCorrectly() {

		boundValueOps.set(VALUE, Duration.ofMillis(250));

		verify(valueOps).set(eq(KEY), eq(VALUE), eq(250L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAREDIS-815
	public void setIfAbsentWithDurationOfSecondsShouldDelegateCorrectly() {

		boundValueOps.setIfAbsent(VALUE, Duration.ofSeconds(1));

		verify(valueOps).setIfAbsent(eq(KEY), eq(VALUE), eq(1L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	public void setIfAbsentWithDurationOfMillisShouldDelegateCorrectly() {

		boundValueOps.setIfAbsent(VALUE, Duration.ofMillis(250));

		verify(valueOps).setIfAbsent(eq(KEY), eq(VALUE), eq(250L), eq(TimeUnit.MILLISECONDS));
	}

	@Test // DATAREDIS-815
	public void setIfPresentWithDurationOfSecondsShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE, Duration.ofSeconds(1));

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE), eq(1L), eq(TimeUnit.SECONDS));
	}

	@Test // DATAREDIS-815
	public void setIfPresentWithDurationOfMillisShouldDelegateCorrectly() {

		boundValueOps.setIfPresent(VALUE, Duration.ofMillis(250));

		verify(valueOps).setIfPresent(eq(KEY), eq(VALUE), eq(250L), eq(TimeUnit.MILLISECONDS));
	}
}
