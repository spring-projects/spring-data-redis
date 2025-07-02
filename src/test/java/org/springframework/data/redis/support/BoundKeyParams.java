/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.support;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class BoundKeyParams {

	public static Collection<Object[]> testParams() {
		// Jedis
		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		StringRedisTemplate templateJS = new StringRedisTemplate(jedisConnFactory);
		DefaultRedisMap mapJS = new DefaultRedisMap("bound:key:map", templateJS);
		DefaultRedisSet setJS = new DefaultRedisSet("bound:key:set", templateJS);
		RedisList list = RedisList.create("bound:key:list", templateJS);

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		StringRedisTemplate templateLT = new StringRedisTemplate(lettuceConnFactory);
		DefaultRedisMap mapLT = new DefaultRedisMap("bound:key:mapLT", templateLT);
		DefaultRedisSet setLT = new DefaultRedisSet("bound:key:setLT", templateLT);
		RedisList listLT = RedisList.create("bound:key:listLT", templateLT);

		StringObjectFactory sof = new StringObjectFactory();

		return Arrays
				.asList(new Object[][] { { new RedisAtomicInteger("bound:key:int", jedisConnFactory), sof, templateJS },
						{ new RedisAtomicLong("bound:key:long", jedisConnFactory), sof, templateJS }, { list, sof, templateJS },
						{ setJS, sof, templateJS }, { mapJS, sof, templateJS },
						{ new RedisAtomicInteger("bound:key:intLT", lettuceConnFactory), sof, templateLT },
						{ new RedisAtomicLong("bound:key:longLT", lettuceConnFactory), sof, templateLT },
						{ listLT, sof, templateLT }, { setLT, sof, templateLT }, { mapLT, sof, templateLT } });
	}
}
