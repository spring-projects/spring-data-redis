/*
 * Copyright 2017-2018 the original author or authors.
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

import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * {@link java.lang.String String-focused} extension of {@link ReactiveRedisTemplate}. As most operations against Redis
 * are {@link String} based, this class provides a dedicated arrangement that minimizes configuration of its more
 * generic {@link ReactiveRedisTemplate template} especially in terms of the used {@link RedisSerializationContext}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
public class ReactiveStringRedisTemplate extends ReactiveRedisTemplate<String, String> {

	/**
	 * Creates new {@link ReactiveRedisTemplate} using given {@link ReactiveRedisConnectionFactory} applying default
	 * {@link String} serialization.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @see RedisSerializationContext#string()
	 */
	public ReactiveStringRedisTemplate(ReactiveRedisConnectionFactory connectionFactory) {
		this(connectionFactory, RedisSerializationContext.string());
	}

	/**
	 * Creates new {@link ReactiveRedisTemplate} using given {@link ReactiveRedisConnectionFactory} and
	 * {@link RedisSerializationContext}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 */
	public ReactiveStringRedisTemplate(ReactiveRedisConnectionFactory connectionFactory,
			RedisSerializationContext<String, String> serializationContext) {
		super(connectionFactory, serializationContext);
	}

	/**
	 * Creates new {@link ReactiveRedisTemplate} using given {@link ReactiveRedisConnectionFactory} and
	 * {@link RedisSerializationContext}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param serializationContext must not be {@literal null}.
	 * @param exposeConnection flag indicating to expose the connection used.
	 */
	public ReactiveStringRedisTemplate(ReactiveRedisConnectionFactory connectionFactory,
			RedisSerializationContext<String, String> serializationContext, boolean exposeConnection) {
		super(connectionFactory, serializationContext, exposeConnection);
	}
}
