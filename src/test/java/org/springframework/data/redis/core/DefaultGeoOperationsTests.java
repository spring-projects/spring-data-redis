/*
 * Copyright 2013-2014 the original author or authors.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnection;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.springframework.data.redis.SpinBarrier.waitFor;
import static org.springframework.data.redis.matcher.RedisTestMatchers.isEqual;

/**
 * Integration test of {@link org.springframework.data.redis.core.DefaultValueOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author David Liu
 * @author Thomas Darimont
 */
@RunWith(Parameterized.class)
public class DefaultGeoOperationsTests<K, V> {

	private RedisTemplate<K, V> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<V> valueFactory;

	private GeoOperations<K, V> geoOperations;

	public DefaultGeoOperationsTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
                                     ObjectFactory<V> valueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@Before
	public void setUp() {
		geoOperations = redisTemplate.opsForGeo();
	}

	@After
	public void tearDown() {
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Test
	public void testGeoAdd() throws Exception {
		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		Long numAdded = geoOperations.geoAdd(key, 13.361389, 38.115556, v1);
		assertEquals(numAdded.longValue(), 1L);

//        numAdded = geoOperations.geoAdd("Sicily", 15.087269, 37.502669, "Catania");
//		assertEquals(numAdded.longValue(), 2L);
	}
}
