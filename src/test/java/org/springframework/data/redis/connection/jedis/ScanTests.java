/*
 * Copyright 2016-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@MethodSource("params")
public class ScanTests {

	private RedisConnectionFactory factory;
	private RedisTemplate<String, String> redisOperations;

	private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 1, TimeUnit.MINUTES,
			new LinkedBlockingDeque<>());

	public ScanTests(RedisConnectionFactory factory) {
		this.factory = factory;
	}

	public static List<RedisConnectionFactory> params() {

		JedisConnectionFactory jedisConnectionFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		LettuceConnectionFactory lettuceConnectionFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		return Arrays.asList(jedisConnectionFactory, lettuceConnectionFactory);
	}

	@BeforeEach
	void setUp() {

		redisOperations = new StringRedisTemplate(factory);
		redisOperations.afterPropertiesSet();
	}

	@ParameterizedRedisTest
	void contextLoads() throws InterruptedException {

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
				} catch (Exception ex) {
					exception.set(ex);
				}
			});
		}

		// Wait until work is finished
		while (executor.getActiveCount() > 0) {
			Thread.sleep(100);
		}

		executor.shutdown();

		assertThat(exception.get()).isNull();
	}
}
