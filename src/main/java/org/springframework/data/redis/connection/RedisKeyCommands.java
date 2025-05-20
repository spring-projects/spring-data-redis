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
package org.springframework.data.redis.connection;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyScanOptions;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Key-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 */
@NullUnmarked
public interface RedisKeyCommands {

	/**
	 * Copy given {@code sourceKey} to {@code targetKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param targetKey must not be {@literal null}.
	 * @param replace whether to replace existing keys.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/copy">Redis Documentation: COPY</a>
	 * @since 2.6
	 */
	Boolean copy(byte @NonNull [] sourceKey, byte @NonNull [] targetKey, boolean replace);

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if key exists. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	default Boolean exists(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");
		Long count = exists(new byte[][] { key });
		return count != null ? count > 0 : null;
	}

	/**
	 * Count how many of the given {@code keys} exist. Providing the very same {@code key} more than once also counts
	 * multiple times.
	 *
	 * @param keys must not be {@literal null}.
	 * @return the number of keys existing among the ones specified as arguments. {@literal null} when used in pipeline /
	 *         transaction.
	 * @since 2.1
	 */
	Long exists(byte @NonNull [] @NonNull... keys);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Long del(byte @NonNull [] @NonNull... keys);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #del(byte[]...)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Long unlink(byte @NonNull [] @NonNull... keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	DataType type(byte @NonNull [] key);

	/**
	 * Alter the last access time of given {@code key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/touch">Redis Documentation: TOUCH</a>
	 * @since 2.1
	 */
	Long touch(byte @NonNull [] @NonNull... keys);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return empty {@link Set} if no match found. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	Set<byte @NonNull []> keys(byte @NonNull [] pattern);

	/**
	 * Use a {@link Cursor} to iterate over keys.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	default Cursor<byte @NonNull []> scan(@NonNull KeyScanOptions options) {
		return scan((ScanOptions) options);
	}

	/**
	 * Use a {@link Cursor} to iterate over keys.
	 *
	 * @param options can be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	Cursor<byte @NonNull []> scan(@Nullable ScanOptions options);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return {@literal null} if no keys available or when used in pipeline or transaction.
	 * @see <a href="https://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	byte[] randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	void rename(byte @NonNull [] oldKey, byte @NonNull [] newKey);

	/**
	 * Rename key {@code oldKey} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	Boolean renameNX(byte @NonNull [] oldKey, byte @NonNull [] newKey);

	/**
	 * @param key must not be {@literal null}.
	 * @param expiration the {@link org.springframework.data.redis.core.types.Expiration} to apply.
	 * @param options additional options to be sent along with the command.
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 * @see <a href="https://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	default Boolean applyExpiration(byte @NonNull [] key,
			org.springframework.data.redis.core.types.@NonNull Expiration expiration, @NonNull ExpirationOptions options) {

		if (expiration.isPersistent()) {
			return persist(key);
		}

		if (ObjectUtils.nullSafeEquals(ExpirationOptions.none(), options)) {
			if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
				if (expiration.isUnixTimestamp()) {
					return expireAt(key, expiration.getExpirationTimeInMilliseconds());
				}
				return expire(key, expiration.getExpirationTimeInMilliseconds());
			}
			if (expiration.isUnixTimestamp()) {
				return expireAt(key, expiration.getExpirationTimeInSeconds());
			}
			return expire(key, expiration.getExpirationTimeInSeconds());
		}

		if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
			if (expiration.isUnixTimestamp()) {
				return expireAt(key, expiration.getExpirationTimeInMilliseconds(), options.getCondition());
			}

			return expire(key, expiration.getExpirationTimeInMilliseconds(), options.getCondition());
		}

		if (expiration.isUnixTimestamp()) {
			return expireAt(key, expiration.getExpirationTimeInSeconds(), options.getCondition());
		}

		return expire(key, expiration.getExpirationTimeInSeconds(), options.getCondition());
	}

	/**
	 * Set time to live for given {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 */
	default Boolean expire(byte @NonNull [] key, long seconds) {
		return expire(key, seconds, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set time to live for given {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @since 3.5
	 */
	Boolean expire(byte @NonNull [] key, long seconds, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set time to live for given {@code key} using {@link Duration#toSeconds() seconds} precision.
	 *
	 * @param key must not be {@literal null}.
	 * @param duration
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @since 3.5
	 */
	default Boolean expire(byte @NonNull [] key, @NonNull Duration duration) {
		return expire(key, duration.toSeconds());
	}

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 */
	default Boolean pExpire(byte @NonNull [] key, long millis) {
		return pExpire(key, millis, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 * @since 3.5
	 */
	Boolean pExpire(byte @NonNull [] key, long millis, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set time to live for given {@code key} using {@link Duration#toMillis() milliseconds} precision.
	 *
	 * @param key must not be {@literal null}.
	 * @param duration
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 * @since 3.5
	 */
	default Boolean pExpire(byte @NonNull [] key, @NonNull Duration duration) {
		return pExpire(key, duration.toMillis());
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 */
	default Boolean expireAt(byte @NonNull [] key, long unixTime) {
		return expireAt(key, unixTime, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 * @since 3.5
	 */
	Boolean expireAt(byte @NonNull [] key, long unixTime, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in {@link Instant#getEpochSecond() seconds}
	 * precision.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 * @since 3.5
	 */
	default Boolean expireAt(byte @NonNull [] key, @NonNull Instant unixTime) {
		return expireAt(key, unixTime.getEpochSecond());
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 */
	default Boolean pExpireAt(byte @NonNull [] key, long unixTimeInMillis) {
		return pExpireAt(key, unixTimeInMillis, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 * @since 3.5
	 */
	Boolean pExpireAt(byte @NonNull [] key, long unixTimeInMillis, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in {@link Instant#toEpochMilli()
	 * milliseconds} precision.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @return {@literal null} when used in pipeline / transaction. {@literal true} if the timeout was set or
	 *         {@literal false} if the timeout was not set; for example, the key doesn't exist, or the operation was
	 *         skipped because of the provided arguments.
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 * @since 3.5
	 */
	default Boolean pExpireAt(byte @NonNull [] key, @NonNull Instant unixTime) {
		return pExpireAt(key, unixTime.toEpochMilli());
	}

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	Boolean persist(byte @NonNull [] key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	Boolean move(byte @NonNull [] key, int dbIndex);

	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	Long ttl(byte @NonNull [] key);

	/**
	 * Get the time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	Long ttl(byte @NonNull [] key, @NonNull TimeUnit timeUnit);

	/**
	 * Get the precise time to live for {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	Long pTtl(byte @NonNull [] key);

	/**
	 * Get the precise time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	Long pTtl(byte @NonNull [] key, @NonNull TimeUnit timeUnit);

	/**
	 * Sort the elements for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param params can be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	List<byte @NonNull []> sort(byte @NonNull [] key, @Nullable SortParameters params);

	/**
	 * Sort the elements for {@code key} and store result in {@code storeKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param params can be {@literal null}.
	 * @param storeKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	Long sort(byte @NonNull [] key, @Nullable SortParameters params, byte @NonNull [] storeKey);

	/**
	 * Retrieve serialized version of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/dump">Redis Documentation: DUMP</a>
	 */
	byte[] dump(byte @NonNull [] key);

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(byte[])}.
	 *
	 * @param key must not be {@literal null}.
	 * @param ttlInMillis
	 * @param serializedValue must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	default void restore(byte @NonNull [] key, long ttlInMillis, byte @NonNull [] serializedValue) {
		restore(key, ttlInMillis, serializedValue, false);
	}

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(byte[])}.
	 *
	 * @param key must not be {@literal null}.
	 * @param ttlInMillis
	 * @param serializedValue must not be {@literal null}.
	 * @param replace use {@literal true} to replace a potentially existing value instead of erroring.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	void restore(byte @NonNull [] key, long ttlInMillis, byte @NonNull [] serializedValue, boolean replace);

	/**
	 * Get the type of internal representation used for storing the value at the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@link org.springframework.data.redis.connection.ValueEncoding.RedisValueEncoding#VACANT} if key does not
	 *         exist or {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/object">Redis Documentation: OBJECT ENCODING</a>
	 * @since 2.1
	 */
	ValueEncoding encodingOf(byte @NonNull [] key);

	/**
	 * Get the {@link Duration} since the object stored at the given {@code key} is idle.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/object">Redis Documentation: OBJECT IDLETIME</a>
	 * @since 2.1
	 */
	Duration idletime(byte @NonNull [] key);

	/**
	 * Get the number of references of the value associated with the specified {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/object">Redis Documentation: OBJECT REFCOUNT</a>
	 * @since 2.1
	 */
	Long refcount(byte @NonNull [] key);

}
