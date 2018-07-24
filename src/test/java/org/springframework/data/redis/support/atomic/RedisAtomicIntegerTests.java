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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * Integration test of {@link RedisAtomicInteger}
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisAtomicIntegerTests extends AbstractRedisAtomicsTests {

	private RedisAtomicInteger intCounter;
	private RedisConnectionFactory factory;
	private RedisTemplate<String, Integer> template;

	public RedisAtomicIntegerTests(RedisConnectionFactory factory) {

		this.intCounter = new RedisAtomicInteger(getClass().getSimpleName() + ":int", factory);
		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Integer.class));
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

		intCounter.set(0);
		assertThat(intCounter.compareAndSet(1, 10)).isFalse();
		assertThat(intCounter.compareAndSet(0, 10)).isTrue();
		assertThat(intCounter.compareAndSet(10, 0)).isTrue();
	}

	@Test
	public void testIncrementAndGet() {

		intCounter.set(0);
		assertThat(intCounter.incrementAndGet()).isOne();
	}

	@Test
	public void testAddAndGet() {

		intCounter.set(0);
		int delta = 5;
		assertThat(intCounter.addAndGet(delta)).isEqualTo(delta);
	}

	@Test
	public void testDecrementAndGet() {

		intCounter.set(1);
		assertThat(intCounter.decrementAndGet()).isZero();
	}

	@Test // DATAREDIS-469
	public void testGetAndIncrement() {

		intCounter.set(1);
		assertThat(intCounter.getAndIncrement()).isOne();
		assertThat(intCounter.get()).isEqualTo(2);
	}

	@Test // DATAREDIS-469
	public void testGetAndAdd() {

		intCounter.set(1);
		assertThat(intCounter.getAndAdd(5)).isOne();
		assertThat(intCounter.get()).isEqualTo(6);
	}

	@Test // DATAREDIS-469
	public void testGetAndDecrement() {

		intCounter.set(1);
		assertThat(intCounter.getAndDecrement()).isOne();
		assertThat(intCounter.get()).isZero();
	}

	@Test // DATAREDIS-469
	public void testGetAndSet() {

		intCounter.set(1);
		assertThat(intCounter.getAndSet(5)).isOne();
		assertThat(intCounter.get()).isEqualTo(5);
	}

	@Test // DATAREDIS-108, DATAREDIS-843
	public void testCompareSet() throws Exception {

		AtomicBoolean alreadySet = new AtomicBoolean(false);
		int NUM = 50;
		String KEY = getClass().getSimpleName() + ":atomic:counter";
		CountDownLatch latch = new CountDownLatch(NUM);
		AtomicBoolean failed = new AtomicBoolean(false);
		RedisAtomicInteger atomicInteger = new RedisAtomicInteger(KEY, factory);

		for (int i = 0; i < NUM; i++) {

			new Thread(() -> {

				try {
					if (atomicInteger.compareAndSet(0, 1)) {
						if (alreadySet.get()) {
							failed.set(true);
						}
						alreadySet.set(true);
					}
				} finally {
					latch.countDown();
				}
			}).start();
		}

		latch.await();

		assertThat(failed.get()).withFailMessage("counter already modified").isFalse();
	}

	@Test // DATAREDIS-317
	public void testShouldThrowExceptionIfRedisAtomicIntegerIsUsedWithRedisTemplateAndNoKeySerializer() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("a valid key serializer in template is required");

		new RedisAtomicInteger("foo", new RedisTemplate<>());
	}

	@Test // DATAREDIS-317
	public void testShouldThrowExceptionIfRedisAtomicIntegerIsUsedWithRedisTemplateAndNoValueSerializer() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("a valid value serializer in template is required");

		RedisTemplate<String, Integer> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		new RedisAtomicInteger("foo", template);
	}

	@Test // DATAREDIS-317
	public void testShouldBeAbleToUseRedisAtomicIntegerWithProperlyConfiguredRedisTemplate() {

		RedisAtomicInteger ral = new RedisAtomicInteger("DATAREDIS-317.atomicInteger", template);
		ral.set(32);

		assertThat(ral.get()).isEqualTo(32);
	}

	@Test // DATAREDIS-469
	public void getThrowsExceptionWhenKeyHasBeenRemoved() {

		expectedException.expect(DataRetrievalFailureException.class);
		expectedException.expectMessage("'test' seems to no longer exist");

		// setup integer
		RedisAtomicInteger test = new RedisAtomicInteger("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		test.get();
	}

	@Test // DATAREDIS-469
	public void getAndSetReturnsZeroWhenKeyHasBeenRemoved() {

		// setup integer
		RedisAtomicInteger test = new RedisAtomicInteger("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		assertThat(test.getAndSet(2)).isZero();
	}
}
