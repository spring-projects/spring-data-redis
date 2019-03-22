/*
 * Copyright 2011-2019 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Redis operations for simple (or in Redis terminology 'string') values.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface ValueOperations<K, V> {

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	void set(K key, V value);

	/**
	 * Set the {@code value} and expiration {@code timeout} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	void set(K key, V value, long timeout, TimeUnit unit);

	/**
	 * Set {@code key} to hold the string {@code value} if {@code key} is absent.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/setnx">Redis Documentation: SETNX</a>
	 */
	Boolean setIfAbsent(K key, V value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param map must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/mset">Redis Documentation: MSET</a>
	 */
	void multiSet(Map<? extends K, ? extends V> map);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param map must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/mset">Redis Documentation: MSET</a>
	 */
	Boolean multiSetIfAbsent(Map<? extends K, ? extends V> map);

	/**
	 * Get the value of {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	V get(Object key);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	V getAndSet(K key, V value);

	/**
	 * Get multiple {@code keys}. Values are returned in the order of the requested keys.
	 *
	 * @param keys must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/mget">Redis Documentation: MGET</a>
	 */
	List<V> multiGet(Collection<K> keys);

	/**
	 * Increment an integer value stored as string value under {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @see <a href="http://redis.io/commands/incr">Redis Documentation: INCR</a>
	 */
	Long increment(K key, long delta);

	/**
	 * Increment a floating point number value stored as string value under {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @see <a href="http://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 */
	Double increment(K key, double delta);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @see <a href="http://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	Integer append(K key, String value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="http://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	String get(K key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="http://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	void set(K key, V value, long offset);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 */
	Long size(K key);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/setbit">Redis Documentation: SETBIT</a>
	 */
	Boolean setBit(K key, long offset, boolean value);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/setbit">Redis Documentation: GETBIT</a>
	 */
	Boolean getBit(K key, long offset);

	RedisOperations<K, V> getOperations();

}
