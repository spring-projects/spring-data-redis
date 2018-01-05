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
package org.springframework.data.redis.support.collections;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.AbstractRedisCollection;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisStore;

/**
 * Parameterized instance of Redis tests.
 *
 * @author Costin Leau
 */
public class RedisSetTests extends AbstractRedisSetTests<Object> {

	/**
	 * Constructs a new <code>RedisSetTests</code> instance.
	 *
	 * @param factory
	 * @param template
	 */
	public RedisSetTests(ObjectFactory<Object> factory, RedisTemplate template) {
		super(factory, template);
	}

	RedisStore copyStore(RedisStore store) {
		return new DefaultRedisSet(store.getKey().toString(), store.getOperations());
	}

	AbstractRedisCollection<Object> createCollection() {
		String redisName = getClass().getName();
		return new DefaultRedisSet(redisName, template);
	}
}
