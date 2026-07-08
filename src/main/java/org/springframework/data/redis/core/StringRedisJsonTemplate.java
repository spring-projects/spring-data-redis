/*
 * Copyright 2026-present the original author or authors.
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

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * String-focused extension of {@link RedisJsonTemplate}. Since String works naturally with JSON, this provides users
 * with a default String template for JSONs which accepts String keys.
 *
 * @author Yordan Tsintsov
 */
public class StringRedisJsonTemplate extends RedisJsonTemplate<String> {

	/**
	 * Creates a new {@link StringRedisJsonTemplate} using the given {@link RedisConnectionFactory} and default
	 * serializers.
	 * <p>
	 * Keys are serialized using the {@link RedisSerializer#string() UTF-8 String serializer}, producing human-readable
	 * keys. The JSON serializer defaults to a Jackson 3-based serializer and therefore requires {@code tools.jackson} on
	 * the classpath.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @throws IllegalStateException if no supported JSON library is available on the classpath.
	 */
	public StringRedisJsonTemplate(RedisConnectionFactory connectionFactory) {
		super(connectionFactory, RedisSerializer.string(), RedisJsonTemplate.defaultJsonSerializer());
	}

	/**
	 * Creates a new {@link StringRedisJsonTemplate} using the given {@link RedisConnectionFactory} and serializers.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @param keySerializer must not be {@literal null}.
	 * @param jsonSerializer must not be {@literal null}.
	 */
	public StringRedisJsonTemplate(RedisConnectionFactory connectionFactory, RedisSerializer<String> keySerializer, RedisJsonSerializer jsonSerializer) {
		super(connectionFactory, keySerializer, jsonSerializer);
	}

}
