/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.support.atomic;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration test of {@link RedisAtomicLong}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisAtomicLongTests extends AbstractRedisAtomicsTests {

	private RedisAtomicLong longCounter;
	private RedisConnectionFactory factory;
	private RedisTemplate<String, Long> template;

	public RedisAtomicLongTests(RedisConnectionFactory factory) {

		this.longCounter = new RedisAtomicLong(getClass().getSimpleName() + ":long", factory);
		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		this.template.afterPropertiesSet();

		ConnectionFactoryTracker.add(factory);
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AtomicCountersParam.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@After
	public void tearDown() {

		RedisConnection connection = factory.getConnection();
		connection.flushDb();
		connection.close();
	}

	@Test
	public void testCheckAndSet() {

		longCounter.set(0);
		assertThat(longCounter.compareAndSet(1, 10)).isFalse();
		assertThat(longCounter.compareAndSet(0, 10)).isTrue();
		assertThat(longCounter.compareAndSet(10, 0)).isTrue();
	}

	@Test
	public void testIncrementAndGet() {

		longCounter.set(0);
		assertThat(longCounter.incrementAndGet()).isOne();
	}

	@Test
	public void testAddAndGet() {

		longCounter.set(0);
		long delta = 5;
		assertThat(longCounter.addAndGet(delta)).isEqualTo(delta);
	}

	@Test
	public void testDecrementAndGet() {

		longCounter.set(1);
		assertThat(longCounter.decrementAndGet()).isZero();
	}

	@Test // DATAREDIS-469
	public void testGetAndIncrement() {

		longCounter.set(1);
		assertThat(longCounter.getAndIncrement()).isOne();
		assertThat(longCounter.get()).isEqualTo(2);
	}

	@Test // DATAREDIS-469
	public void testGetAndAdd() {

		longCounter.set(1);
		assertThat(longCounter.getAndAdd(5)).isOne();
		assertThat(longCounter.get()).isEqualTo(6);
	}

	@Test // DATAREDIS-469
	public void testGetAndDecrement() {

		longCounter.set(1);
		assertThat(longCounter.getAndDecrement()).isOne();
		assertThat(longCounter.get()).isZero();
	}

	@Test // DATAREDIS-469
	public void testGetAndSet() {

		longCounter.set(1);
		assertThat(longCounter.getAndSet(5)).isOne();
		assertThat(longCounter.get()).isEqualTo(5);
	}

	@Test
	public void testGetExistingValue() {

		longCounter.set(5);
		RedisAtomicLong keyCopy = new RedisAtomicLong(longCounter.getKey(), factory);
		assertThat(longCounter.get()).isEqualTo(keyCopy.get());
	}

	@Test // DATAREDIS-317
	public void testShouldThrowExceptionIfAtomicLongIsUsedWithRedisTemplateAndNoKeySerializer() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("a valid key serializer in template is required");

		RedisTemplate<String, Long> template = new RedisTemplate<>();
		new RedisAtomicLong("foo", template);
	}

	@Test // DATAREDIS-317
	public void testShouldThrowExceptionIfAtomicLongIsUsedWithRedisTemplateAndNoValueSerializer() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("a valid value serializer in template is required");

		RedisTemplate<String, Long> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		new RedisAtomicLong("foo", template);
	}

	@Test // DATAREDIS-317
	public void testShouldBeAbleToUseRedisAtomicLongWithProperlyConfiguredRedisTemplate() {

		RedisTemplate<String, Long> template = new RedisTemplate<>();
		template.setConnectionFactory(factory);
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		template.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		template.afterPropertiesSet();

		RedisAtomicLong ral = new RedisAtomicLong("DATAREDIS-317.atomicLong", template);
		ral.set(32L);

		assertThat(ral.get()).isEqualTo(32L);
	}

	@Test // DATAREDIS-469
	public void getThrowsExceptionWhenKeyHasBeenRemoved() {

		expectedException.expect(DataRetrievalFailureException.class);
		expectedException.expectMessage("'test' seems to no longer exist");

		// setup long
		RedisAtomicLong test = new RedisAtomicLong("test", factory, 1);
		assertThat(test.get()).isOne();

		template.delete("test");

		test.get();
	}

	@Test // DATAREDIS-469
	public void getAndSetReturnsZeroWhenKeyHasBeenRemoved() {

		// setup long
		RedisAtomicLong test = new RedisAtomicLong("test", factory, 1);
		assertThat(test.get()).isOne();

		template.delete("test");

		assertThat(test.getAndSet(2)).isZero();
	}
}
