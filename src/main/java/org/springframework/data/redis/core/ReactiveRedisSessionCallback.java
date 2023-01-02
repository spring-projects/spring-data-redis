/*
 * Copyright 2021-2023 the original author or authors.
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

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ReactiveRedisConnection;

/**
 * Generic callback interface for code that wants to use the same {@link ReactiveRedisConnection} avoiding connection
 * allocation overhead upon each Template API method call. Allows to execute any number of operations on a single
 * {@link ReactiveRedisConnection}, using any type and number of commands.
 * <p>
 * This is particularly useful for issuing multiple calls on the same connection.
 *
 * @param <T>
 * @author Mark Paluch
 * @since 2.6
 * @see ReactiveRedisOperations#executeInSession(ReactiveRedisSessionCallback)
 */
public interface ReactiveRedisSessionCallback<K, V, T> {

	/**
	 * Gets called by {@link ReactiveRedisOperations#executeInSession(ReactiveRedisSessionCallback)} with an active Redis
	 * connection. Does not need to care about activating or closing the {@link ReactiveRedisConnection}.
	 * <p>
	 * Allows for returning a result object created within the callback, i.e. a domain object or a collection of domain
	 * objects.
	 *
	 * @param operations template associated with a connection.
	 * @return a result object {@link Publisher}.
	 * @throws DataAccessException in case of custom exceptions.
	 */
	Publisher<T> doWithOperations(ReactiveRedisOperations<K, V> operations) throws DataAccessException;
}
