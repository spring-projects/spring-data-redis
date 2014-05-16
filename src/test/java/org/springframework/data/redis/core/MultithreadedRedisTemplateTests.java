/*
 * Copyright 2014 the original author or authors.
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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;

/**
 * @author Artem Bilian
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class MultithreadedRedisTemplateTests {

	private RedisConnectionFactory factory;

	public MultithreadedRedisTemplateTests(RedisConnectionFactory factory) {
		this.factory = factory;
	}

	@Parameters
	public static Collection<Object[]> testParams() {

		JedisConnectionFactory jedis = new JedisConnectionFactory();
		jedis.setPort(6379);
		jedis.afterPropertiesSet();

		LettuceConnectionFactory lettuce = new LettuceConnectionFactory();
		lettuce.setPort(6379);
		lettuce.afterPropertiesSet();

		SrpConnectionFactory srp = new SrpConnectionFactory();
		srp.setPort(6479);
		srp.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { jedis }, { lettuce }, { srp } });
	}

	/**
	 * @see DATAREDIS-300
	 */
	@Test
	public void assertResouresAreReleasedProperlyWhenSharingRedisTemplate() throws InterruptedException {

		final RedisTemplate<Object, Object> template = new RedisTemplate<Object, Object>();
		template.setConnectionFactory(factory);
		template.afterPropertiesSet();

		ExecutorService executor = Executors.newCachedThreadPool();

		for (int i = 0; i < 9; i++) {
			executor.execute(new Runnable() {

				@Override
				public void run() {
					template.boundValueOps("foo").get();
				}
			});
		}

		executor.shutdown();
		assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
	}

}
