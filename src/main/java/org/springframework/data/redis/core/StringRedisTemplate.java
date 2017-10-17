/*
 * Copyright 2011-2013 the original author or authors.
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

import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * String-focused extension of RedisTemplate. Since most operations against Redis are String based, this class provides
 * a dedicated class that minimizes configuration of its more generic {@link RedisTemplate template} especially in terms
 * of serializers.
 * <p/>
 * Note that this template exposes the {@link RedisConnection} used by the {@link RedisCallback} as a
 * {@link StringRedisConnection}.
 * 
 * @author Costin Leau
 * @author Mark Paluch
 */
public class StringRedisTemplate extends RedisTemplate<String, String> {

	/**
	 * Constructs a new <code>StringRedisTemplate</code> instance. {@link #setConnectionFactory(RedisConnectionFactory)}
	 * and {@link #afterPropertiesSet()} still need to be called.
	 */
	public StringRedisTemplate() {
		setKeySerializer(StringRedisSerializer.UTF_8);
		setValueSerializer(StringRedisSerializer.UTF_8);
		setHashKeySerializer(StringRedisSerializer.UTF_8);
		setHashValueSerializer(StringRedisSerializer.UTF_8);
	}

	/**
	 * Constructs a new <code>StringRedisTemplate</code> instance ready to be used.
	 * 
	 * @param connectionFactory connection factory for creating new connections
	 */
	public StringRedisTemplate(RedisConnectionFactory connectionFactory) {
		this();
		setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	protected RedisConnection preProcessConnection(RedisConnection connection, boolean existingConnection) {
		return new DefaultStringRedisConnection(connection);
	}
}
