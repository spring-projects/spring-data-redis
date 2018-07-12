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
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.assertj.core.data.Offset;
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
 * Integration test of {@link RedisAtomicDouble}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisAtomicDoubleTests extends AbstractRedisAtomicsTests {

	private RedisAtomicDouble doubleCounter;
	private RedisConnectionFactory factory;
	private RedisTemplate<String, Double> template;

	public RedisAtomicDoubleTests(RedisConnectionFactory factory) {

		this.doubleCounter = new RedisAtomicDouble(getClass().getSimpleName() + ":double", factory);
		this.factory = factory;

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		this.template.setKeySerializer(StringRedisSerializer.UTF_8);
		this.template.setValueSerializer(new GenericToStringSerializer<>(Double.class));
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

	@Test // DATAREDIS-198
	public void testCheckAndSet() {

		doubleCounter.set(0);
		assertThat(doubleCounter.compareAndSet(1.2, 10.6)).isFalse();
		assertThat(doubleCounter.compareAndSet(0, 10.6)).isTrue();
		assertThat(doubleCounter.compareAndSet(10.6, 0)).isTrue();
	}

	@Test // DATAREDIS-198
	public void testIncrementAndGet() {

		doubleCounter.set(0);
		assertThat(doubleCounter.incrementAndGet()).isEqualTo(1.0);
	}

	@Test // DATAREDIS-198
	public void testAddAndGet() {

		doubleCounter.set(0);
		double delta = 1.3;
		assertThat(doubleCounter.addAndGet(delta)).isCloseTo(delta, Offset.offset(.0001));
	}

	@Test // DATAREDIS-198
	public void testDecrementAndGet() {

		doubleCounter.set(1);
		assertThat(doubleCounter.decrementAndGet()).isZero();
	}

	@Test // DATAREDIS-198
	public void testGetAndSet() {

		doubleCounter.set(3.4);
		assertThat(doubleCounter.getAndSet(1.2)).isEqualTo(3.4);
		assertThat(doubleCounter.get()).isEqualTo(1.2);
	}

	@Test // DATAREDIS-198
	public void testGetAndIncrement() {

		doubleCounter.set(2.3);
		assertThat(doubleCounter.getAndIncrement()).isEqualTo(2.3);
		assertThat(doubleCounter.get()).isEqualTo(3.3);
	}

	@Test // DATAREDIS-198
	public void testGetAndDecrement() {

		doubleCounter.set(0.5);
		assertThat(doubleCounter.getAndDecrement()).isEqualTo(0.5);
		assertThat(doubleCounter.get()).isEqualTo(-0.5);
	}

	@Test // DATAREDIS-198
	public void testGetAndAdd() {

		doubleCounter.set(0.5);
		assertThat(doubleCounter.getAndAdd(0.7)).isEqualTo(0.5);
		assertThat(doubleCounter.get()).isEqualTo(1.2);
	}

	@Test // DATAREDIS-198
	public void testExpire() {

		assertThat(doubleCounter.expire(1, TimeUnit.SECONDS)).isTrue();
		assertThat(doubleCounter.getExpire() > 0).isTrue();
	}

	@Test // DATAREDIS-198
	public void testExpireAt() {

		doubleCounter.set(7.8);
		assertThat(doubleCounter.expireAt(new Date(System.currentTimeMillis() + 10000))).isTrue();
		assertThat(doubleCounter.getExpire() > 0).isTrue();
	}

	@Test // DATAREDIS-198
	public void testRename() {

		doubleCounter.set(5.6);
		doubleCounter.rename("foodouble");
		assertThat(new String(factory.getConnection().get("foodouble".getBytes()))).isEqualTo("5.6");
		assertThat(factory.getConnection().get((getClass().getSimpleName() + ":double").getBytes())).isNull();
	}

	@Test // DATAREDIS-317
	public void testShouldThrowExceptionIfRedisAtomicDoubleIsUsedWithRedisTemplateAndNoKeySerializer() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("a valid key serializer in template is required");

		new RedisAtomicDouble("foo", new RedisTemplate<>());
	}

	@Test // DATAREDIS-317
	public void testShouldThrowExceptionIfRedisAtomicDoubleIsUsedWithRedisTemplateAndNoValueSerializer() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("a valid value serializer in template is required");

		RedisTemplate<String, Double> template = new RedisTemplate<>();
		template.setKeySerializer(StringRedisSerializer.UTF_8);
		new RedisAtomicDouble("foo", template);
	}

	@Test // DATAREDIS-317
	public void testShouldBeAbleToUseRedisAtomicDoubleWithProperlyConfiguredRedisTemplate() {

		RedisAtomicDouble ral = new RedisAtomicDouble("DATAREDIS-317.atomicDouble", template);
		ral.set(32.23);

		assertThat(ral.get()).isEqualTo(32.23);
	}

	@Test // DATAREDIS-469
	public void getThrowsExceptionWhenKeyHasBeenRemoved() {

		expectedException.expect(DataRetrievalFailureException.class);
		expectedException.expectMessage("'test' seems to no longer exist");

		// setup double
		RedisAtomicDouble test = new RedisAtomicDouble("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		test.get();
	}

	@Test // DATAREDIS-469
	public void getAndSetReturnsZeroWhenKeyHasBeenRemoved() {

		// setup double
		RedisAtomicDouble test = new RedisAtomicDouble("test", factory, 1);
		assertThat(test.get()).isOne(); // this passes

		template.delete("test");

		assertThat(test.getAndSet(2)).isZero();
	}
}
