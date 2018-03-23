/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class ScanTests {

	RedisConnectionFactory factory;
	RedisTemplate<String, String> redisOperations;

	ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 1, TimeUnit.MINUTES,
			new LinkedBlockingDeque<>());

	public ScanTests(RedisConnectionFactory factory) {

		this.factory = factory;
		ConnectionFactoryTracker.add(factory);
	}

	@Parameters
	public static List<RedisConnectionFactory> params() {

		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
		jedisConnectionFactory.setHostName(SettingsUtils.getHost());
		jedisConnectionFactory.setPort(SettingsUtils.getPort());
		jedisConnectionFactory.afterPropertiesSet();

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnectionFactory.setHostName(SettingsUtils.getHost());
		lettuceConnectionFactory.setPort(SettingsUtils.getPort());
		lettuceConnectionFactory.afterPropertiesSet();

		return Arrays.<RedisConnectionFactory> asList(jedisConnectionFactory, lettuceConnectionFactory);
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		redisOperations = new StringRedisTemplate(factory);
		redisOperations.afterPropertiesSet();
	}

	@Test
	public void contextLoads() throws InterruptedException {

		BoundHashOperations<String, String, String> hash = redisOperations.boundHashOps("hash");
		final AtomicReference<Exception> exception = new AtomicReference<>();

		// Create some keys so that SCAN requires a while to return all data.
		for (int i = 0; i < 10000; i++) {
			hash.put("key-" + i, "value");
		}

		// Concurrent access
		for (int i = 0; i < 10; i++) {

			executor.submit(() -> {
				try {

					Cursor<Entry<Object, Object>> cursorMap = redisOperations.boundHashOps("hash")
							.scan(ScanOptions.scanOptions().match("*").count(100).build());

					// This line invokes the lazy SCAN invocation
					while (cursorMap.hasNext()) {
						cursorMap.next();
					}
					cursorMap.close();
				} catch (Exception e) {
					exception.set(e);
				}
			});
		}

		// Wait until work is finished
		while (executor.getActiveCount() > 0) {
			Thread.sleep(100);
		}

		executor.shutdown();

		assertThat(exception.get(), is(nullValue()));
	}
}
