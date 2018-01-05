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
 * Set operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public interface BoundSetOperations<K, V> extends BoundKeyOperations<K> {

	/**
	 * Add given {@code values} to set at the bound key.
	 *
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	@Nullable
	Long add(V... values);

	/**
	 * Remove given {@code values} from set at the bound key and return the number of removed elements.
	 *
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	@Nullable
	Long remove(Object... values);

	/**
	 * Remove and return a random member from set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	@Nullable
	V pop();

	/**
	 * Move {@code value} from the bound key to {@code destKey}
	 *
	 * @param destKey must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	@Nullable
	Boolean move(K destKey, V value);

	/**
	 * Get size of set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	@Nullable
	Long size();

	/**
	 * Check if set at the bound key contains {@code value}.
	 *
	 * @param o
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	@Nullable
	Boolean isMember(Object o);

	/**
	 * Returns the members intersecting all given sets at the bound key and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	@Nullable
	Set<V> intersect(K key);

	/**
	 * Returns the members intersecting all given sets at the bound key and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	@Nullable
	Set<V> intersect(Collection<K> keys);

	/**
	 * Intersect all given sets at the bound key and {@code key} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	void intersectAndStore(K key, K destKey);

	/**
	 * Intersect all given sets at the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	void intersectAndStore(Collection<K> keys, K destKey);

	/**
	 * Union all sets at given {@code key} and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	@Nullable
	Set<V> union(K key);

	/**
	 * Union all sets at given {@code keys} and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	@Nullable
	Set<V> union(Collection<K> keys);

	/**
	 * Union all sets at given the bound key and {@code key} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	void unionAndStore(K key, K destKey);

	/**
	 * Union all sets at given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	void unionAndStore(Collection<K> keys, K destKey);

	/**
	 * Diff all sets for given the bound key and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	@Nullable
	Set<V> diff(K key);

	/**
	 * Diff all sets for given the bound key and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	@Nullable
	Set<V> diff(Collection<K> keys);

	/**
	 * Diff all sets for given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	void diffAndStore(K keys, K destKey);

	/**
	 * Diff all sets for given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	void diffAndStore(Collection<K> keys, K destKey);

	/**
	 * Get all elements of set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	@Nullable
	Set<V> members();

	/**
	 * Get random element from set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	V randomMember();

	/**
	 * Get {@code count} distinct random elements from set at the bound key.
	 *
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	Set<V> distinctRandomMembers(long count);

	/**
	 * Get {@code count} random elements from set at the bound key.
	 *
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	List<V> randomMembers(long count);

	/**
	 * @param options
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.4
	 */
	@Nullable
	Cursor<V> scan(ScanOptions options);

	/**
	 * @return never {@literal null}.
	 */
	RedisOperations<K, V> getOperations();

}
