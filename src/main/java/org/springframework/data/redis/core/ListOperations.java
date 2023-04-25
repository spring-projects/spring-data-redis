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
package org.springframework.data.redis.core;

import static org.springframework.data.redis.connection.RedisListCommands.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Redis list specific operations.
 *
 * @author Costin Leau
 * @author David Liu
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 */
public interface ListOperations<K, V> {

	/**
	 * Get elements between {@code begin} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 */
	@Nullable
	List<V> range(K key, long start, long end);

	/**
	 * Trim list at {@code key} to elements between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="https://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 */
	void trim(K key, long start, long end);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 */
	@Nullable
	Long size(K key);

	/**
	 * Prepend {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long leftPush(K key, V value);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
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
	 * @see <a href="https://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long leftPushAll(K key, Collection<V> values);

	/**
	 * Prepend {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpushx">Redis Documentation: LPUSHX</a>
	 */
	@Nullable
	Long leftPushIfPresent(K key, V value);

	/**
	 * Insert {@code value} to {@code key} before {@code pivot}.
	 *
	 * @param key must not be {@literal null}.
	 * @param pivot must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 */
	@Nullable
	Long leftPush(K key, V pivot, V value);

	/**
	 * Append {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rightPush(K key, V value);

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
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
	 * @see <a href="https://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rightPushAll(K key, Collection<V> values);

	/**
	 * Append {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpushx">Redis Documentation: RPUSHX</a>
	 */
	@Nullable
	Long rightPushIfPresent(K key, V value);

	/**
	 * Insert {@code value} to {@code key} after {@code pivot}.
	 *
	 * @param key must not be {@literal null}.
	 * @param pivot must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 */
	@Nullable
	Long rightPush(K key, V pivot, V value);

	/**
	 * Value object representing the {@code where from} part for the {@code LMOVE} command.
	 *
	 * @param <K>
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/lmove">Redis Documentation: LMOVE</a>
	 */
	class MoveFrom<K> {

		final K key;
		final Direction direction;

		MoveFrom(K key, Direction direction) {

			this.key = key;
			this.direction = direction;
		}

		public static <K> MoveFrom<K> fromHead(K key) {
			return new MoveFrom<>(key, Direction.first());
		}

		public static <K> MoveFrom<K> fromTail(K key) {
			return new MoveFrom<>(key, Direction.last());
		}
	}

	/**
	 * Value object representing the {@code where to} from part for the {@code LMOVE} command.
	 *
	 * @param <K>
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/lmove">Redis Documentation: LMOVE</a>
	 */
	class MoveTo<K> {

		final K key;
		final Direction direction;

		MoveTo(K key, Direction direction) {

			this.key = key;
			this.direction = direction;
		}

		public static <K> MoveTo<K> toHead(K key) {
			return new MoveTo<>(key, Direction.first());
		}

		public static <K> MoveTo<K> toTail(K key) {
			return new MoveTo<>(key, Direction.last());
		}
	}

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 *
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/lmove">Redis Documentation: LMOVE</a>
	 */
	@Nullable
	default V move(MoveFrom<K> from, MoveTo<K> to) {

		Assert.notNull(from, "Move from must not be null");
		Assert.notNull(to, "Move to must not be null");

		return move(from.key, from.direction, to.key, to.direction);
	}

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/lmove">Redis Documentation: LMOVE</a>
	 */
	@Nullable
	V move(K sourceKey, Direction from, K destinationKey, Direction to);

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param timeout must not be {@literal null} or negative.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/blmove">Redis Documentation: BLMOVE</a>
	 */
	@Nullable
	default V move(MoveFrom<K> from, MoveTo<K> to, Duration timeout) {

		Assert.notNull(from, "Move from must not be null");
		Assert.notNull(to, "Move to must not be null");
		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return move(from.key, from.direction, to.key, to.direction,
				TimeoutUtils.toMillis(timeout.toMillis(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
	}

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param timeout must not be {@literal null} or negative.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/blmove">Redis Documentation: BLMOVE</a>
	 */
	@Nullable
	default V move(K sourceKey, Direction from, K destinationKey, Direction to, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return move(sourceKey, from, destinationKey, to, TimeoutUtils.toMillis(timeout.toMillis(), TimeUnit.MILLISECONDS),
				TimeUnit.MILLISECONDS);
	}

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param timeout
	 * @param unit
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/blmove">Redis Documentation: BLMOVE</a>
	 */
	@Nullable
	V move(K sourceKey, Direction from, K destinationKey, Direction to, long timeout, TimeUnit unit);

	/**
	 * Set the {@code value} list element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value
	 * @see <a href="https://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	void set(K key, long index, V value);

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	@Nullable
	Long remove(K key, long count, Object value);

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	@Nullable
	V index(K key, long index);

	/**
	 * Returns the index of the first occurrence of the specified value in the list at at {@code key}. <br />
	 * Requires Redis 6.0.6 or newer.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction or when not contained in list.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/lpos">Redis Documentation: LPOS</a>
	 */
	@Nullable
	Long indexOf(K key, V value);

	/**
	 * Returns the index of the last occurrence of the specified value in the list at at {@code key}. <br />
	 * Requires Redis 6.0.6 or newer.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction or when not contained in list.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/lpos">Redis Documentation: LPOS</a>
	 */
	@Nullable
	Long lastIndexOf(K key, V value);

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 */
	@Nullable
	V leftPop(K key);

	/**
	 * Removes and returns first {@code} elements in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 * @since 2.6
	 */
	@Nullable
	List<V> leftPop(K key, long count);

	/**
	 * Removes and returns first element from lists stored at {@code key} . <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 */
	@Nullable
	V leftPop(K key, long timeout, TimeUnit unit);

	/**
	 * Removes and returns first element from lists stored at {@code key} . <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @since 2.3
	 * @see <a href="https://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 */
	@Nullable
	default V leftPop(K key, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return leftPop(key, TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	@Nullable
	V rightPop(K key);

	/**
	 * Removes and returns last {@code} elements in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 * @since 2.6
	 */
	@Nullable
	List<V> rightPop(K key, long count);

	/**
	 * Removes and returns last element from lists stored at {@code key}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	@Nullable
	V rightPop(K key, long timeout, TimeUnit unit);

	/**
	 * Removes and returns last element from lists stored at {@code key}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.3
	 * @see <a href="https://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	@Nullable
	default V rightPop(K key, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return rightPop(key, TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	/**
	 * Remove the last element from list at {@code sourceKey}, append it to {@code destinationKey} and return its value.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	@Nullable
	V rightPopAndLeftPush(K sourceKey, K destinationKey);

	/**
	 * Remove the last element from list at {@code sourceKey}, append it to {@code destinationKey} and return its value.<br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	@Nullable
	V rightPopAndLeftPush(K sourceKey, K destinationKey, long timeout, TimeUnit unit);

	/**
	 * Remove the last element from list at {@code sourceKey}, append it to {@code destinationKey} and return its value.<br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @throws IllegalArgumentException if the timeout is {@literal null} or negative.
	 * @since 2.3
	 * @see <a href="https://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	@Nullable
	default V rightPopAndLeftPush(K sourceKey, K destinationKey, Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");
		Assert.isTrue(!timeout.isNegative(), "Timeout must not be negative");

		return rightPopAndLeftPush(sourceKey, destinationKey, TimeoutUtils.toSeconds(timeout), TimeUnit.SECONDS);
	}

	RedisOperations<K, V> getOperations();
}
