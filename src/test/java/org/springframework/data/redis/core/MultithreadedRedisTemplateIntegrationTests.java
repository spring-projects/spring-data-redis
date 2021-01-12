/*
 * Copyright 2014-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * @author Artem Bilian
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("testParams")
public class MultithreadedRedisTemplateIntegrationTests {

	private final RedisConnectionFactory factory;

	public MultithreadedRedisTemplateIntegrationTests(RedisConnectionFactory factory) {
		this.factory = factory;
	}

	public static Collection<Object> testParams() {

		JedisConnectionFactory jedis = JedisConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class);
		LettuceConnectionFactory lettuce = LettuceConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class);

		return Arrays.asList(jedis, lettuce);
	}

	@ParameterizedRedisTest // DATAREDIS-300
	void assertResouresAreReleasedProperlyWhenSharingRedisTemplate() throws InterruptedException {

		final RedisTemplate<Object, Object> template = new RedisTemplate<>();
		template.setConnectionFactory(factory);
		template.afterPropertiesSet();

		ExecutorService executor = Executors.newCachedThreadPool();

		for (int i = 0; i < 9; i++) {
			executor.execute(template.boundValueOps("foo")::get);
		}

		executor.shutdown();
		assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
	}

}
