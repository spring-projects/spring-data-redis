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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Redis list specific operations.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see <a href="http://redis.io/commands#list">Redis Documentation: List Commands</a>
 * @since 2.0
 */
public interface ReactiveListOperations<K, V> {

	/**
	 * Get elements between {@code begin} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 */
	Flux<V> range(K key, long start, long end);

	/**
	 * Trim list at {@code key} to elements between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="http://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 */
	Mono<Boolean> trim(K key, long start, long end);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 */
	Mono<Long> size(K key);

	/**
	 * Prepend {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	Mono<Long> leftPush(K key, V value);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	Mono<Long> leftPushAll(K key, V... values);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	Mono<Long> leftPushAll(K key, Collection<V> values);

	/**
	 * Prepend {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/lpushx">Redis Documentation: LPUSHX</a>
	 */
	Mono<Long> leftPushIfPresent(K key, V value);

	/**
	 * Prepend {@code values} to {@code key} before {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	Mono<Long> leftPush(K key, V pivot, V value);

	/**
	 * Append {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	Mono<Long> rightPush(K key, V value);

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	Mono<Long> rightPushAll(K key, V... values);

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @since 1.5
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	Mono<Long> rightPushAll(K key, Collection<V> values);

	/**
	 * Append {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/rpushx">Redis Documentation: RPUSHX</a>
	 */
	Mono<Long> rightPushIfPresent(K key, V value);

	/**
	 * Append {@code values} to {@code key} before {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: RPUSH</a>
	 */
	Mono<Long> rightPush(K key, V pivot, V value);

	/**
	 * Set the {@code value} list element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value
	 * @see <a href="http://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	Mono<Boolean> set(K key, long index, V value);

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	Mono<Long> remove(K key, long count, Object value);

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return
	 * @see <a href="http://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	Mono<V> index(K key, long index);

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 */
	Mono<V> leftPop(K key);

	/**
	 * Removes and returns first element from lists stored at {@code key}. <br>
	 * <b>Results return once an element available or {@code timeout} reached.</b>
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout maximal duration to wait until an entry in the list at {@code key} is available. Must be either
	 *          {@link Duration#ZERO} or greater {@link 1 second}, must not be {@literal null}. A timeout of zero can be
	 *          used to wait indefinitely. Durations between zero and one second are not supported.
	 * @return
	 * @see <a href="http://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 */
	Mono<V> leftPop(K key, Duration timeout);

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	Mono<V> rightPop(K key);

	/**
	 * Removes and returns last element from lists stored at {@code key}. <br>
	 * <b>Results return once an element available or {@code timeout} reached.</b>
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout maximal duration to wait until an entry in the list at {@code key} is available. Must be either
	 *          {@link Duration#ZERO} or greater {@link 1 second}, must not be {@literal null}. A timeout of zero can be
	 *          used to wait indefinitely. Durations between zero and one second are not supported.
	 * @return
	 * @see <a href="http://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	Mono<V> rightPop(K key, Duration timeout);

	/**
	 * Remove the last element from list at {@code sourceKey}, append it to {@code destinationKey} and return its value.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	Mono<V> rightPopAndLeftPush(K sourceKey, K destinationKey);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value.<br>
	 * <b>Results return once an element available or {@code timeout} reached.</b>
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param timeout maximal duration to wait until an entry in the list at {@code sourceKey} is available. Must be
	 *          either {@link Duration#ZERO} or greater {@link 1 second}, must not be {@literal null}. A timeout of zero
	 *          can be used to wait indefinitely. Durations between zero and one second are not supported.
	 * @return
	 * @see <a href="http://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	Mono<V> rightPopAndLeftPush(K sourceKey, K destinationKey, Duration timeout);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	Mono<Boolean> delete(K key);
}
