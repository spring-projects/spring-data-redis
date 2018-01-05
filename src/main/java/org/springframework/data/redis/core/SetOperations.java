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
import java.util.Set;

import org.springframework.lang.Nullable;

/**
 * Redis set specific operations.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface SetOperations<K, V> {

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	@Nullable
	Long add(K key, V... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	@Nullable
	Long remove(K key, Object... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	@Nullable
	V pop(K key);

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of random members to pop from the set.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 * @since 2.0
	 */
	@Nullable
	List<V> pop(K key, long count);

	/**
	 * Move {@code value} from {@code key} to {@code destKey}
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	@Nullable
	Boolean move(K key, V value, K destKey);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	@Nullable
	Long size(K key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	@Nullable
	Boolean isMember(K key, Object o);

	/**
	 * Returns the members intersecting all given sets at {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	@Nullable
	Set<V> intersect(K key, K otherKey);

	/**
	 * Returns the members intersecting all given sets at {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	@Nullable
	Set<V> intersect(K key, Collection<K> otherKeys);

	/**
	 * Intersect all given sets at {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	@Nullable
	Long intersectAndStore(K key, K otherKey, K destKey);

	/**
	 * Intersect all given sets at {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	@Nullable
	Long intersectAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Union all sets at given {@code keys} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	@Nullable
	Set<V> union(K key, K otherKey);

	/**
	 * Union all sets at given {@code keys} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	@Nullable
	Set<V> union(K key, Collection<K> otherKeys);

	/**
	 * Union all sets at given {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	@Nullable
	Long unionAndStore(K key, K otherKey, K destKey);

	/**
	 * Union all sets at given {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	@Nullable
	Long unionAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	@Nullable
	Set<V> difference(K key, K otherKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	@Nullable
	Set<V> difference(K key, Collection<K> otherKeys);

	/**
	 * Diff all sets for given {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	@Nullable
	Long differenceAndStore(K key, K otherKey, K destKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	@Nullable
	Long differenceAndStore(K key, Collection<K> otherKeys, K destKey);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	@Nullable
	Set<V> members(K key);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	V randomMember(K key);

	/**
	 * Get {@code count} distinct random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count nr of members to return
	 * @return empty {@link Set} if {@code key} does not exist.
	 * @throws IllegalArgumentException if count is negative.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	Set<V> distinctRandomMembers(K key, long count);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count nr of members to return.
	 * @return empty {@link List} if {@code key} does not exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if count is negative.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	List<V> randomMembers(K key, long count);

	/**
	 * Iterate over elements in set at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leak.
	 *
	 * @param key
	 * @param options
	 * @return
	 * @since 1.4
	 */
	Cursor<V> scan(K key, ScanOptions options);

	RedisOperations<K, V> getOperations();
}
