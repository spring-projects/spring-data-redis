/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.util;

import java.util.UUID;

import org.springframework.datastore.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.datastore.redis.core.RedisOperations;
import org.springframework.datastore.redis.core.RedisTemplate;


/**
 * String-based Redis List test.
 * 
 * @author Costin Leau
 */
public class StringRedisListTest extends AbstractRedisListTest<String> {

	private JedisConnectionFactory factory;

	@Override
	AbstractRedisCollection<String> createCollection() {
		String redisName = getClass().getName();
		factory = new JedisConnectionFactory();
		factory.setPooling(false);
		factory.afterPropertiesSet();

		RedisTemplate<String, String> template = new RedisTemplate<String, String>(factory);
		return new DefaultRedisList<String>(redisName, template);
	}

	@Override
	void destroyCollection() {
		factory.destroy();
	}

	@Override
	RedisStore<String> copyStore(RedisStore<String> store) {
		return new DefaultRedisList<String>(store.getKey(), (RedisOperations<String, String>) store.getOperations());
	}

	@Override
	String getT() {
		return UUID.randomUUID().toString();
	}
}

