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
package org.springframework.data.redis.core;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Callback interface for Redis 'low level' code. To be used with {@link RedisTemplate} execution methods, often as
 * anonymous classes within a method implementation. Usually, used for chaining several operations together (
 * {@code get/set/trim etc...}.
 *
 * @author Costin Leau
 * @author John Blum
 */
public interface RedisCallback<T extends @Nullable Object> {

	/**
	 * Method called by {@link RedisTemplate} with an active {@link RedisConnection}.
	 * <p>
	 * Callback code need not care about activating/opening or closing the {@link RedisConnection}, nor handling
	 * {@link Exception exceptions}.
	 *
	 * @param connection active {@link RedisConnection Redis connection}.
	 * @return the {@link Object result} of the operation performed in the callback or {@literal null}.
	 * @throws DataAccessException if the operation performed by the callback fails to execute in the context of Redis
	 *           using the given {@link RedisConnection}.
	 */
	T doInRedis(RedisConnection connection) throws DataAccessException;

}
