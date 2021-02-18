/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.redis.listener;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.test.condition.RedisDetector;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisStanalone;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class PubSubTestParams {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getNewConnectionFactory(RedisStanalone.class);

		jedisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate(jedisConnFactory);
		RedisTemplate<String, Person> personTemplate = new RedisTemplate<>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();
		RedisTemplate<byte[], byte[]> rawTemplate = new RedisTemplate<>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.afterPropertiesSet();

		// add Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStanalone.class);

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate(lettuceConnFactory);
		RedisTemplate<String, Person> personTemplateLtc = new RedisTemplate<>();
		personTemplateLtc.setConnectionFactory(lettuceConnFactory);
		personTemplateLtc.afterPropertiesSet();
		RedisTemplate<byte[], byte[]> rawTemplateLtc = new RedisTemplate<>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.afterPropertiesSet();

		Collection<Object[]> parameters = new ArrayList<>();
		parameters.add(new Object[] { stringFactory, stringTemplate });
		parameters.add(new Object[] { personFactory, personTemplate });
		parameters.add(new Object[] { stringFactory, stringTemplateLtc });
		parameters.add(new Object[] { personFactory, personTemplateLtc });

		if (clusterAvailable()) {

			// add Jedis
			JedisConnectionFactory jedisClusterFactory = JedisConnectionFactoryExtension
					.getNewConnectionFactory(RedisCluster.class);

			RedisTemplate<String, String> jedisClusterStringTemplate = new StringRedisTemplate(jedisClusterFactory);

			// add Lettuce
			LettuceConnectionFactory lettuceClusterFactory = LettuceConnectionFactoryExtension
					.getConnectionFactory(RedisCluster.class);

			RedisTemplate<String, String> lettuceClusterStringTemplate = new StringRedisTemplate(lettuceClusterFactory);

			parameters.add(new Object[] { stringFactory, jedisClusterStringTemplate });
			parameters.add(new Object[] { stringFactory, lettuceClusterStringTemplate });
		}

		return parameters;
	}

	private static boolean clusterAvailable() {
		return RedisDetector.isClusterAvailable();
	}
}
