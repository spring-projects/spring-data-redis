/*
 * Copyright 2011-2018 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 * @author Christoph Strobl
 */
abstract class AtomicCountersParam {

	static Collection<Object[]> testParams() {

		// Jedis
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory(new RedisStandaloneConfiguration());
		jedisConnFactory.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(),
				LettuceClientConfiguration.builder().clientResources(LettuceTestClientResources.getSharedClientResources())
						.build());
		lettuceConnFactory.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { jedisConnFactory }, { lettuceConnFactory } });
	}
}
