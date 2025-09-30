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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

/**
 * Redis set specific operations.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Roman Bezpalko
 * @author Mingi Lee
 */
@NullUnmarked
public interface SetOperations<K, V> {

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	Long add(@NonNull K key, V @NonNull... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	Long remove(@NonNull K key, Object @NonNull... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	V pop(@NonNull K key);

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of random members to pop from the set.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 * @since 2.0
	 */
	List<@NonNull V> pop(@NonNull K key, long count);

	/**
	 * Move {@code value} from {@code key} to {@code destKey}
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	Boolean move(@NonNull K key, V value, @NonNull K destKey);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	Long size(@NonNull K key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param o
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	Boolean isMember(@NonNull K key, Object o);

	/**
	 * Check if set at {@code key} contains one or more {@code values}.
	 *
	 * @param key must not be {@literal null}.
	 * @param objects
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/smismember">Redis Documentation: SMISMEMBER</a>
	 */
	Map<Object, Boolean> isMember(@NonNull K key, Object @NonNull... objects);

	/**
	 * Returns the members intersecting all given sets at {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Set<@NonNull V> intersect(@NonNull K key, @NonNull K otherKey);

	/**
	 * Returns the members intersecting all given sets at {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Set<@NonNull V> intersect(@NonNull K key, @NonNull Collection<K> otherKeys);

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 * @since 2.2
	 */
	Set<@NonNull V> intersect(@NonNull Collection<K> keys);

	/**
	 * Intersect all given sets at {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey);

	/**
	 * Intersect all given sets at {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	Long intersectAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 * @since 2.2
	 */
	Long intersectAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey);

	/**
	 * Returns the cardinality of the set which would result from the intersection of {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @since 4.0
	 */
	Long intersectSize(@NonNull K key, @NonNull K otherKey);

	/**
	 * Returns the cardinality of the set which would result from the intersection of {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @since 4.0
	 */
	Long intersectSize(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Returns the cardinality of the set which would result from the intersection of all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @since 4.0
	 */
	Long intersectSize(@NonNull Collection<@NonNull K> keys);

	/**
	 * Union all sets at given {@code keys} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Set<@NonNull V> union(@NonNull K key, @NonNull K otherKey);

	/**
	 * Union all sets at given {@code keys} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Set<@NonNull V> union(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Union all sets at given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 * @since 2.2
	 */
	Set<@NonNull V> union(@NonNull Collection<@NonNull K> keys);

	/**
	 * Union all sets at given {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey);

	/**
	 * Union all sets at given {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	Long unionAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Union all sets at given {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 * @since 2.2
	 */
	Long unionAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Set<@NonNull V> difference(@NonNull K key, @NonNull K otherKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Set<@NonNull V> difference(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys);

	/**
	 * Diff all sets for given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 * @since 2.2
	 */
	Set<@NonNull V> difference(@NonNull Collection<@NonNull K> keys);

	/**
	 * Diff all sets for given {@code key} and {@code otherKey} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	Long differenceAndStore(@NonNull K key, @NonNull K otherKey, @NonNull K destKey);

	/**
	 * Diff all sets for given {@code key} and {@code otherKeys} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param otherKeys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	Long differenceAndStore(@NonNull K key, @NonNull Collection<@NonNull K> otherKeys, @NonNull K destKey);

	/**
	 * Diff all sets for given {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 * @since 2.2
	 */
	Long differenceAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	Set<@NonNull V> members(@NonNull K key);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	V randomMember(@NonNull K key);

	/**
	 * Get {@code count} distinct random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count nr of members to return
	 * @return empty {@link Set} if {@code key} does not exist.
	 * @throws IllegalArgumentException if count is negative.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	Set<@NonNull V> distinctRandomMembers(@NonNull K key, long count);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count nr of members to return.
	 * @return empty {@link List} if {@code key} does not exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if count is negative.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	List<@NonNull V> randomMembers(@NonNull K key, long count);

	/**
	 * Use a {@link Cursor} to iterate over entries set at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param key
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	@NonNull
	Cursor<@NonNull V> scan(@NonNull K key, @NonNull ScanOptions options);

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, V> getOperations();

}
