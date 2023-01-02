/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.springframework.data.redis.core.RedisOperations;

/**
 * Redis extension for the {@link Set} contract. Supports {@link Set} specific operations backed by Redis operations.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisSet<E> extends RedisCollection<E>, Set<E> {

	/**
	 * Constructs a new {@link RedisSet} instance.
	 *
	 * @param key Redis key of this set.
	 * @param operations {@link RedisOperations} for the value type of this set.
	 * @since 2.6
	 */
	static <E> RedisSet<E> create(String key, RedisOperations<String, E> operations) {
		return new DefaultRedisSet<>(key, operations);
	}

	/**
	 * Diff this set and another {@link RedisSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the values that differ.
	 * @since 1.0
	 */
	Set<E> diff(RedisSet<?> set);

	/**
	 * Diff this set and other {@link RedisSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the values that differ.
	 * @since 1.0
	 */
	Set<E> diff(Collection<? extends RedisSet<?>> sets);

	/**
	 * Create a new {@link RedisSet} by diffing this sorted set and {@link RedisSet} and store result in destination
	 * {@code destKey}.
	 *
	 * @param set must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisSet} pointing at {@code destKey}.
	 * @since 1.0
	 */
	RedisSet<E> diffAndStore(RedisSet<?> set, String destKey);

	/**
	 * Create a new {@link RedisSet} by diffing this sorted set and the collection {@link RedisSet} and store result in
	 * destination {@code destKey}.
	 *
	 * @param sets must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisSet} pointing at {@code destKey}.
	 * @since 1.0
	 */
	RedisSet<E> diffAndStore(Collection<? extends RedisSet<?>> sets, String destKey);

	/**
	 * Intersect this set and another {@link RedisSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the intersecting values.
	 * @since 1.0
	 */
	Set<E> intersect(RedisSet<?> set);

	/**
	 * Intersect this set and other {@link RedisSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the intersecting values.
	 * @since 1.0
	 */
	Set<E> intersect(Collection<? extends RedisSet<?>> sets);

	/**
	 * Create a new {@link RedisSet} by intersecting this sorted set and {@link RedisSet} and store result in destination
	 * {@code destKey}.
	 *
	 * @param set must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisSet} pointing at {@code destKey}
	 * @since 1.0
	 */
	RedisSet<E> intersectAndStore(RedisSet<?> set, String destKey);

	/**
	 * Create a new {@link RedisSet} by intersecting this sorted set and the collection {@link RedisSet} and store result
	 * in destination {@code destKey}.
	 *
	 * @param sets must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisSet} pointing at {@code destKey}.
	 * @since 1.0
	 */
	RedisSet<E> intersectAndStore(Collection<? extends RedisSet<?>> sets, String destKey);

	/**
	 * Get random element from the set.
	 *
	 * @return
	 * @since 2.6
	 */
	E randomValue();

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<E> scan();

	/**
	 * Union this set and another {@link RedisSet}.
	 *
	 * @param set must not be {@literal null}.
	 * @return a {@link Set} containing the combined values.
	 * @since 2.6
	 */
	Set<E> union(RedisSet<?> set);

	/**
	 * Union this set and other {@link RedisSet}s.
	 *
	 * @param sets must not be {@literal null}.
	 * @return a {@link Set} containing the combined values.
	 * @since 1.0
	 */
	Set<E> union(Collection<? extends RedisSet<?>> sets);

	/**
	 * Create a new {@link RedisSet} by union this sorted set and {@link RedisSet} and store result in destination
	 * {@code destKey}.
	 *
	 * @param set must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisSet} pointing at {@code destKey}.
	 * @since 1.0
	 */
	RedisSet<E> unionAndStore(RedisSet<?> set, String destKey);

	/**
	 * Create a new {@link RedisSet} by union this sorted set and the collection {@link RedisSet} and store result in
	 * destination {@code destKey}.
	 *
	 * @param sets must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @return a new {@link RedisSet} pointing at {@code destKey}.
	 * @since 1.0
	 */
	RedisSet<E> unionAndStore(Collection<? extends RedisSet<?>> sets, String destKey);
}
