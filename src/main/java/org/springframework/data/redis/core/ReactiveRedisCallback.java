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

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ReactiveRedisConnection;

/**
 * Generic callback interface for code that operates on a low-level {@link ReactiveRedisConnection}. Allows to execute
 * any number of operations on a single {@link ReactiveRedisConnection}, using any type and number of commands.
 * <p>
 * This is particularly useful for delegating to existing data access code that expects a
 * {@link ReactiveRedisConnection} to work on. For newly written code, it is strongly recommended to use
 * {@link ReactiveRedisOperations}'s more specific operations.
 *
 * @param <T>
 * @author Mark Paluch
 * @since 2.0
 * @see ReactiveRedisOperations#execute(ReactiveRedisCallback)
 */
public interface ReactiveRedisCallback<T> {

	/**
	 * Gets called by {@link ReactiveRedisTemplate#execute(ReactiveRedisCallback)} with an active Redis connection. Does
	 * not need to care about activating or closing the {@link ReactiveRedisConnection}.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects.
	 *
	 * @param connection active Redis connection.
	 * @return a result object publisher
	 * @throws DataAccessException in case of custom exceptions
	 */
	Publisher<T> doInRedis(ReactiveRedisConnection connection) throws DataAccessException;
}
