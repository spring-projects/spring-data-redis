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
 * Set operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Mingi Lee
 */
@NullUnmarked
public interface BoundSetOperations<K, V> extends BoundKeyOperations<K> {

	/**
	 * Add given {@code values} to set at the bound key.
	 *
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	Long add(V @NonNull... values);

	/**
	 * Remove given {@code values} from set at the bound key and return the number of removed elements.
	 *
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	Long remove(Object @NonNull... values);

	/**
	 * Remove and return a random member from set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	V pop();

	/**
	 * Move {@code value} from the bound key to {@code destKey}
	 *
	 * @param destKey must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	Boolean move(@NonNull K destKey, @NonNull V value);

	/**
	 * Get size of set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	Long size();

	/**
	 * Check if set at the bound key contains {@code value}.
	 *
	 * @param o
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	Boolean isMember(@NonNull Object o);

	/**
	 * Check if set at at the bound key contains one or more {@code values}.
	 *
	 * @param objects
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/smismember">Redis Documentation: SMISMEMBER</a>
	 */
	Map<Object, Boolean> isMember(Object @NonNull... objects);

	/**
	 * Returns the members intersecting all given sets at the bound key and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Set<@NonNull V> intersect(@NonNull K key);

	/**
	 * Returns the members intersecting all given sets at the bound key and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Set<@NonNull V> intersect(@NonNull Collection<@NonNull K> keys);

	/**
	 * Intersect all given sets at the bound key and {@code key} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	void intersectAndStore(@NonNull K key, @NonNull K destKey);

	/**
	 * Intersect all given sets at the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	void intersectAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey);

	/**
	 * Returns the cardinality of the set which would result from the intersection of the bound key and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @since 4.0
	 */
	Long intersectSize(@NonNull K key);

	/**
	 * Returns the cardinality of the set which would result from the intersection of the bound key and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @since 4.0
	 */
	Long intersectSize(@NonNull Collection<@NonNull K> keys);

	/**
	 * Union all sets at given {@code key} and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Set<@NonNull V> union(@NonNull K key);

	/**
	 * Union all sets at given {@code keys} and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Set<@NonNull V> union(@NonNull Collection<@NonNull K> keys);

	/**
	 * Union all sets at given the bound key and {@code key} and store result in {@code destKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	void unionAndStore(@NonNull K key, @NonNull K destKey);

	/**
	 * Union all sets at given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	void unionAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey);

	/**
	 * Diff all sets for given the bound key and {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Set<@NonNull V> difference(@NonNull K key);

	/**
	 * Diff all sets for given the bound key and {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Set<@NonNull V> difference(@NonNull Collection<@NonNull K> keys);

	/**
	 * Diff all sets for given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 * @deprecated since 3.0, use {@link #differenceAndStore(Object, Object)} instead to follow a consistent method naming
	 *             scheme.
	 */
	@Deprecated
	default void diffAndStore(@NonNull K keys, @NonNull K destKey) {
		differenceAndStore(keys, destKey);
	}

	/**
	 * Diff all sets for given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	void differenceAndStore(@NonNull K keys, @NonNull K destKey);

	/**
	 * Diff all sets for given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 * @deprecated since 3.0, use {@link #differenceAndStore(Collection, Object)} instead to follow a consistent method
	 *             naming scheme.
	 */
	@Deprecated
	default void diffAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey) {
		differenceAndStore(keys, destKey);
	}

	/**
	 * Diff all sets for given the bound key and {@code keys} and store result in {@code destKey}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	void differenceAndStore(@NonNull Collection<@NonNull K> keys, @NonNull K destKey);

	/**
	 * Get all elements of set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	Set<@NonNull V> members();

	/**
	 * Get random element from set at the bound key.
	 *
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	V randomMember();

	/**
	 * Get {@code count} distinct random elements from set at the bound key.
	 *
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	Set<@NonNull V> distinctRandomMembers(long count);

	/**
	 * Get {@code count} random elements from set at the bound key.
	 *
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	List<@NonNull V> randomMembers(long count);

	/**
	 * Use a {@link Cursor} to iterate over entries in set at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 1.4
	 */
	Cursor<@NonNull V> scan(@NonNull ScanOptions options);

	/**
	 * @return the underlying {@link RedisOperations} used to execute commands.
	 */
	@NonNull
	RedisOperations<K, V> getOperations();

}
