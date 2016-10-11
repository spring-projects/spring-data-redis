/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.redis.support;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundKeyOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class BoundKeyOperationsTest {

	@SuppressWarnings("rawtypes")//
	private BoundKeyOperations keyOps;

	private ObjectFactory<Object> objFactory;

	@SuppressWarnings("rawtypes")//
	private RedisTemplate template;

	@SuppressWarnings("rawtypes")
	public BoundKeyOperationsTest(BoundKeyOperations<Object> keyOps, ObjectFactory<Object> objFactory,
			RedisTemplate template) {
		this.objFactory = objFactory;
		this.keyOps = keyOps;
		this.template = template;
		ConnectionFactoryTracker.add(template.getConnectionFactory());
	}

	@Before
	public void setUp() {
		populateBoundKey();
	}

	@SuppressWarnings("unchecked")
	@After
	public void tearDown() {
		template.execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.flushDb();
				return null;
			}
		});
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return BoundKeyParams.testParams();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRename() throws Exception {

		Object key = keyOps.getKey();
		Object newName = objFactory.instance();

		keyOps.rename(newName);
		assertThat(keyOps.getKey()).isEqualTo(newName);

		keyOps.rename(key);
		assertThat(keyOps.getKey()).isEqualTo(key);
	}

	@Test // DATAREDIS-251
	public void testExpire() throws Exception {

		assertThat(keyOps.getExpire()).as(keyOps.getClass().getName() + " -> " + keyOps.getKey()).isEqualTo(Long.valueOf(-1));

		if (keyOps.expire(10, TimeUnit.SECONDS)) {
			long expire = keyOps.getExpire().longValue();
			assertThat(expire).isGreaterThan(5).isLessThanOrEqualTo(10);
		}
	}

	@Test // DATAREDIS-251
	public void testPersist() throws Exception {
		assumeTrue(!ConnectionUtils.isJredis(template.getConnectionFactory()));

		keyOps.persist();

		assertThat(keyOps.getExpire()).as(keyOps.getClass().getName() + " -> " + keyOps.getKey()).isEqualTo( Long.valueOf(-1));
		if (keyOps.expire(10, TimeUnit.SECONDS)) {
			assertThat(keyOps.getExpire()).isGreaterThan(0);
		}

		keyOps.persist();
		assertThat(keyOps.getExpire().longValue()).as(keyOps.getClass().getName() + " -> " + keyOps.getKey()).isEqualTo(-1);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void populateBoundKey() {
		if (keyOps instanceof Collection) {
			((Collection) keyOps).add("dummy");
		} else if (keyOps instanceof Map) {
			((Map) keyOps).put("dummy", "dummy");
		} else if (keyOps instanceof RedisAtomicInteger) {
			((RedisAtomicInteger) keyOps).set(42);
		} else if (keyOps instanceof RedisAtomicLong) {
			((RedisAtomicLong) keyOps).set(42L);
		}
	}
}
