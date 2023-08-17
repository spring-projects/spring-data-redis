/*
 * Copyright 2017-2023 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Reactive Redis operations for Set Commands.
 * <p>
 * Streams of methods returning {@code Mono<K>} or {@code Flux<M>} are terminated with
 * {@link org.springframework.dao.InvalidDataAccessApiUsageException} when
 * {@link org.springframework.data.redis.serializer.RedisElementReader#read(ByteBuffer)} returns {@code null} for a
 * particular element as Reactive Streams prohibit the usage of {@code null} values.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Roman Bezpalko
 * @see <a href="https://redis.io/commands#set">Redis Documentation: Set Commands</a>
 * @since 2.0
 */
public interface ReactiveSetOperations<K, V> {

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="https://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	Mono<Long> add(K key, V... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="https://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	Mono<Long> remove(K key, Object... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	Mono<V> pop(K key);

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of random members to pop from the set.
	 * @return {@link Flux} emitting random members.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	Flux<V> pop(K key, long count);

	/**
	 * Move {@code value} from {@code key} to {@code destKey}
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param value
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	Mono<Boolean> move(K sourceKey, V value, K destKey);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	Mono<Long> size(K key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o
	 * @return
	 * @see <a href="https://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	Mono<Boolean> isMember(K key, Object o);

	/**
	 * Check if set at {@code key} contains one or more {@code values}.
	 *
	 * @param key must not be {@literal null}.
	 * @param objects
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/smismember">Redis Documentation: SMISMEMBER</a>
	 */
	Mono<Map<Object, Boolean>> isMember(K key, Object... objects);

	/**
	 * Returns the members intersecting all given sets at {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Flux<V> intersect(K key, K otherKey);

	/**
	 * Returns the members intersecting all given sets at {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Flux<V> intersect(K key, Collection<K> otherKeys);

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 * @since 2.2
	 */
	Flux<V> intersect(Collection<K> keys);

	/**
	 * Intersect all given sets at {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	Mono<Long> intersectAndStore(K key, K otherKey, K destKey);

	/**
	 * Intersect all given sets at {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	Mono<Long> intersectAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 * @since 2.2
	 */
	Mono<Long> intersectAndStore(Collection<K> keys, K destKey);

	/**
	 * Union all sets at given {@code keys} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Flux<V> union(K key, K otherKey);

	/**
	 * Union all sets at given {@code keys} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Flux<V> union(K key, Collection<K> otherKeys);

	/**
	 * Union all sets at given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 * @since 2.2
	 */
	Flux<V> union(Collection<K> keys);

	/**
	 * Union all sets at given {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(K key, K otherKey, K destKey);

	/**
	 * Union all sets at given {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	Mono<Long> unionAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Union all sets at given {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 * @since 2.2
	 */
	Mono<Long> unionAndStore(Collection<K> keys, K destKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Flux<V> difference(K key, K otherKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Flux<V> difference(K key, Collection<K> otherKeys);

	/**
	 * Diff all sets for given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 * @since 2.2
	 */
	Flux<V> difference(Collection<K> keys);

	/**
	 * Diff all sets for given {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	Mono<Long> differenceAndStore(K key, K otherKey, K destKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	Mono<Long> differenceAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Diff all sets for given {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 * @since 2.2
	 */
	Mono<Long> differenceAndStore(Collection<K> keys, K destKey);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	Flux<V> members(K key);

	/**
	 * Use a {@link Flux} to iterate over entries in the set at {@code key}. The resulting {@link Flux} acts as a cursor
	 * and issues {@code SSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Flux} emitting the {@literal values} one by one or an {@link Flux#empty() empty Flux} if none
	 *         exist.
	 * @throws IllegalArgumentException when given {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/sscan">Redis Documentation: SSCAN</a>
	 * @since 2.1
	 */
	default Flux<V> scan(K key) {
		return scan(key, ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over entries in the set at {@code key} given {@link ScanOptions}. The resulting
	 * {@link Flux} acts as a cursor and issues {@code SSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the {@literal values} one by one or an {@link Flux#empty() empty Flux} if the key
	 *         does not exist.
	 * @throws IllegalArgumentException when one of the required arguments is {@literal null}.
	 * @see <a href="https://redis.io/commands/sscan">Redis Documentation: SSCAN</a>
	 * @since 2.1
	 */
	Flux<V> scan(K key, ScanOptions options);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	Mono<V> randomMember(K key);

	/**
	 * Get {@code count} distinct random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	Flux<V> distinctRandomMembers(K key, long count);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of members to return.
	 * @return
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	Flux<V> randomMembers(K key, long count);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	Mono<Boolean> delete(K key);
}
