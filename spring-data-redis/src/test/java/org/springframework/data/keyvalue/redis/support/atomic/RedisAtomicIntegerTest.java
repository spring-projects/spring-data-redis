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
import org.springframework.data.keyvalue.redis.ConnFactoryTracker;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;

/**
 * @author Costin Leau
 */
@RunWith(Parameterized.class)
public class RedisAtomicIntegerTest {

	private RedisAtomicInteger counter;
	private RedisConnectionFactory factory;


	public RedisAtomicIntegerTest(RedisConnectionFactory factory) {
		counter = new RedisAtomicInteger(getClass().getSimpleName(), factory);
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
		ConnFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AtomicCountersParam.testParams();
	}

	@Test
	public void testCheckAndSet() throws Exception {
		counter.set(0);
		assertFalse(counter.compareAndSet(1, 10));
		assertTrue(counter.compareAndSet(0, 10));
		assertTrue(counter.compareAndSet(10, 0));
	}
}
