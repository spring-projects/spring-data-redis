/*
 * Copyright 2017-2019 the original author or authors.
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

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Reactive Redis operations for simple (or in Redis terminology 'string') values.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveValueOperations<K, V> {

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Mono<Boolean> set(K key, V value);

	/**
	 * Set the {@code value} and expiration {@code timeout} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param timeout must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	Mono<Boolean> set(K key, V value, Duration timeout);

	/**
	 * Set {@code key} to hold the string {@code value} if {@code key} is absent.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/setnx">Redis Documentation: SETNX</a>
	 */
	Mono<Boolean> setIfAbsent(K key, V value);

	/**
	 * Set {@code key} to hold the string {@code value} if {@code key} is present.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Mono<Boolean> setIfPresent(K key, V value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param map must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/mset">Redis Documentation: MSET</a>
	 */
	Mono<Boolean> multiSet(Map<? extends K, ? extends V> map);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param map must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/msetnx">Redis Documentation: MSETNX</a>
	 */
	Mono<Boolean> multiSetIfAbsent(Map<? extends K, ? extends V> map);

	/**
	 * Get the value of {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	Mono<V> get(Object key);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	Mono<V> getAndSet(K key, V value);

	/**
	 * Get multiple {@code keys}. Values are returned in the order of the requested keys.
	 *
	 * @param keys must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/mget">Redis Documentation: MGET</a>
	 */
	Mono<List<V>> multiGet(Collection<K> keys);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	Mono<Long> append(K key, String value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="http://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	Mono<String> get(K key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="http://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	Mono<Long> set(K key, V value, long offset);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 */
	Mono<Long> size(K key);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @see <a href="http://redis.io/commands/setbit">Redis Documentation: SETBIT</a>
	 */
	Mono<Boolean> setBit(K key, long offset, boolean value);

	/**
	 * « Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @see <a href="http://redis.io/commands/setbit">Redis Documentation: GETBIT</a>
	 */
	Mono<Boolean> getBit(K key, long offset);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	Mono<Boolean> delete(K key);
}
