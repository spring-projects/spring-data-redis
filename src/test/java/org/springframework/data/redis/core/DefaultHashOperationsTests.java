/*
 * Copyright 2013 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import static org.junit.Assert.assertEquals;

/**
 * Integration test of {@link DefaultHashOperations}
 *
 * @author Jennifer Hickey
 *
 * @param <K> Key type
 * @param <HK> Hash key type
 * @param <HV> Hash value type
 */
@RunWith(Parameterized.class)
public class DefaultHashOperationsTests<K,HK,HV> {
	private RedisTemplate<K,?> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<HK> hashKeyFactory;

	private ObjectFactory<HV> hashValueFactory;

	private HashOperations<K, HK,HV> hashOps;

	public DefaultHashOperationsTests(RedisTemplate<K,?> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<HK> hashKeyFactory, ObjectFactory<HV> hashValueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.hashKeyFactory = hashKeyFactory;
		this.hashValueFactory = hashValueFactory;
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		SrpConnectionFactory srConnFactory = new SrpConnectionFactory();
		srConnFactory.setPort(SettingsUtils.getPort());
		srConnFactory.setHostName(SettingsUtils.getHost());
		srConnFactory.afterPropertiesSet();
		RedisTemplate<String,String> stringTemplate = new StringRedisTemplate();
		stringTemplate.setConnectionFactory(srConnFactory);
		stringTemplate.afterPropertiesSet();
		return Arrays.asList(new Object[][] { { stringTemplate, stringFactory, stringFactory, stringFactory }});
	}

	@Before
	public void setUp() {
		hashOps = redisTemplate.opsForHash();
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
	public void testEntries() {
		K key = keyFactory.instance();
		HK key1 = hashKeyFactory.instance();
		HV val1 = hashValueFactory.instance();
		HK key2 = hashKeyFactory.instance();
		HV val2 = hashValueFactory.instance();
		hashOps.put(key, key1, val1);
		hashOps.put(key, key2, val2);
		Map<HK,HV> expected = new HashMap<HK,HV>();
		expected.put(key1, val1);
		expected.put(key2, val2);
		assertEquals(expected, hashOps.entries(key));
	}
}
