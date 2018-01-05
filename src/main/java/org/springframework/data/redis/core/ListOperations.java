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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;

/**
 * Redis list specific operations.
 *
 * @author Costin Leau
 * @author David Liu
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface ListOperations<K, V> {

	/**
	 * Get elements between {@code begin} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 */
	@Nullable
	List<V> range(K key, long start, long end);

	/**
	 * Trim list at {@code key} to elements between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="http://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 */
	void trim(K key, long start, long end);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 */
	@Nullable
	Long size(K key);

	/**
	 * Prepend {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long leftPush(K key, V value);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long leftPushAll(K key, V... values);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long leftPushAll(K key, Collection<V> values);

	/**
	 * Prepend {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lpushx">Redis Documentation: LPUSHX</a>
	 */
	@Nullable
	Long leftPushIfPresent(K key, V value);

	/**
	 * Prepend {@code values} to {@code key} before {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long leftPush(K key, V pivot, V value);

	/**
	 * Append {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rightPush(K key, V value);

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rightPushAll(K key, V... values);

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rightPushAll(K key, Collection<V> values);

	/**
	 * Append {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/rpushx">Redis Documentation: RPUSHX</a>
	 */
	@Nullable
	Long rightPushIfPresent(K key, V value);

	/**
	 * Append {@code values} to {@code key} before {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rightPush(K key, V pivot, V value);

	/**
	 * Set the {@code value} list element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value
	 * @see <a href="http://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	void set(K key, long index, V value);

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	@Nullable
	Long remove(K key, long count, Object value);

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	@Nullable
	V index(K key, long index);

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 */
	@Nullable
	V leftPop(K key);

	/**
	 * Removes and returns first element from lists stored at {@code key} . <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 */
	@Nullable
	V leftPop(K key, long timeout, TimeUnit unit);

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	@Nullable
	V rightPop(K key);

	/**
	 * Removes and returns last element from lists stored at {@code key}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	@Nullable
	V rightPop(K key, long timeout, TimeUnit unit);

	/**
	 * Remove the last element from list at {@code sourceKey}, append it to {@code destinationKey} and return its value.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	@Nullable
	V rightPopAndLeftPush(K sourceKey, K destinationKey);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value.<br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	@Nullable
	V rightPopAndLeftPush(K sourceKey, K destinationKey, long timeout, TimeUnit unit);

	RedisOperations<K, V> getOperations();
}
