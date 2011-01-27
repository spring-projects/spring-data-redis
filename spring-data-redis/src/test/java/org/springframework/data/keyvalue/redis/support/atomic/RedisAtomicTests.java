/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.support.atomic;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.keyvalue.redis.ConnectionFactoryTracker;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;

/**
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public class RedisAtomicTests {

	private RedisAtomicInteger intCounter;
	private RedisAtomicLong longCounter;
	private RedisConnectionFactory factory;


	public RedisAtomicTests(RedisConnectionFactory factory) {
		intCounter = new RedisAtomicInteger(getClass().getSimpleName() + ":int", factory);
		longCounter = new RedisAtomicLong(getClass().getSimpleName() + ":long", factory);
		this.factory = factory;
	}

	@After
	public void stop() {
		RedisConnection connection = factory.getConnection();
		connection.flushDb();
		connection.close();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AtomicCountersParam.testParams();
	}

	@Test
	public void testIntCheckAndSet() throws Exception {
		intCounter.set(0);
		assertFalse(intCounter.compareAndSet(1, 10));
		assertTrue(intCounter.compareAndSet(0, 10));
		assertTrue(intCounter.compareAndSet(10, 0));
	}

	@Test
	public void testLongCheckAndSet() throws Exception {
		longCounter.set(0);
		assertFalse(longCounter.compareAndSet(1, 10));
		assertTrue(longCounter.compareAndSet(0, 10));
		assertTrue(longCounter.compareAndSet(10, 0));
	}
}