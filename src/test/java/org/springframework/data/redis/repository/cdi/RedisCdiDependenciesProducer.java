/*
 * Copyright 2016-2021 the original author or authors.
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

package org.springframework.data.redis.repository.cdi;

import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.test.extension.RedisStanalone;

/**
 * @author Mark Paluch
 */
public class RedisCdiDependenciesProducer {

	/**
	 * Provides a producer method for {@link RedisConnectionFactory}.
	 */
	@Produces
	public RedisConnectionFactory redisConnectionFactory() {
		return JedisConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class);
	}

	/**
	 * Provides a producer method for {@link RedisOperations}.
	 */
	@Produces
	public RedisOperations<byte[], byte[]> redisOperationsProducer(RedisConnectionFactory redisConnectionFactory) {

		RedisTemplate<byte[], byte[]> template = new RedisTemplate<>();
		template.setConnectionFactory(redisConnectionFactory);
		template.afterPropertiesSet();
		return template;
	}

	// shortcut for managed KeyValueAdapter/Template.
	@Produces
	@PersonDB
	public RedisOperations<byte[], byte[]> redisOperationsProducerQualified(RedisOperations<byte[], byte[]> instance) {
		return instance;
	}

	public void closeRedisOperations(@Disposes RedisOperations<byte[], byte[]> redisOperations) throws Exception {

		if (redisOperations instanceof DisposableBean) {
			((DisposableBean) redisOperations).destroy();
		}
	}

	/**
	 * Provides a producer method for {@link RedisKeyValueTemplate}.
	 */
	@Produces
	public RedisKeyValueTemplate redisKeyValueAdapterDefault(RedisOperations<?, ?> redisOperations) {

		RedisKeyValueAdapter redisKeyValueAdapter = new RedisKeyValueAdapter(redisOperations);
		RedisKeyValueTemplate keyValueTemplate = new RedisKeyValueTemplate(redisKeyValueAdapter, new RedisMappingContext());
		return keyValueTemplate;
	}

}
