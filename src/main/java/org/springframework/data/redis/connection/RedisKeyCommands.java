/*
 * Copyright 2011-2021 the original author or authors.
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyScanOptions;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Key-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 */
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
	@Nullable
	Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace);

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if key exists. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	@Nullable
	default Boolean exists(byte[] key) {

		Assert.notNull(key, "Key must not be null!");
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
	@Nullable
	Long exists(byte[]... keys);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	@Nullable
	Long del(byte[]... keys);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #del(byte[]...)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	@Nullable
	Long unlink(byte[]... keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	@Nullable
	DataType type(byte[] key);

	/**
	 * Alter the last access time of given {@code key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/touch">Redis Documentation: TOUCH</a>
	 * @since 2.1
	 */
	@Nullable
	Long touch(byte[]... keys);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return empty {@link Set} if no match found. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	@Nullable
	Set<byte[]> keys(byte[] pattern);

	/**
	 * Use a {@link Cursor} to iterate over keys.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	default Cursor<byte[]> scan(KeyScanOptions options) {
		return scan((ScanOptions) options);
	}

	/**
	 * Use a {@link Cursor} to iterate over keys.
	 *
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	Cursor<byte[]> scan(ScanOptions options);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return {@literal null} if no keys available or when used in pipeline or transaction.
	 * @see <a href="https://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	@Nullable
	byte[] randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	void rename(byte[] oldKey, byte[] newKey);

	/**
	 * Rename key {@code oldKey} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	@Nullable
	Boolean renameNX(byte[] oldKey, byte[] newKey);

	/**
	 * Set time to live for given {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 */
	@Nullable
	Boolean expire(byte[] key, long seconds);

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 */
	@Nullable
	Boolean pExpire(byte[] key, long millis);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 */
	@Nullable
	Boolean expireAt(byte[] key, long unixTime);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 */
	@Nullable
	Boolean pExpireAt(byte[] key, long unixTimeInMillis);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	@Nullable
	Boolean persist(byte[] key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	@Nullable
	Boolean move(byte[] key, int dbIndex);

	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	@Nullable
	Long ttl(byte[] key);

	/**
	 * Get the time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	@Nullable
	Long ttl(byte[] key, TimeUnit timeUnit);

	/**
	 * Get the precise time to live for {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	@Nullable
	Long pTtl(byte[] key);

	/**
	 * Get the precise time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	@Nullable
	Long pTtl(byte[] key, TimeUnit timeUnit);

	/**
	 * Sort the elements for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param params must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	List<byte[]> sort(byte[] key, SortParameters params);

	/**
	 * Sort the elements for {@code key} and store result in {@code storeKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param params must not be {@literal null}.
	 * @param storeKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	Long sort(byte[] key, SortParameters params, byte[] storeKey);

	/**
	 * Retrieve serialized version of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/dump">Redis Documentation: DUMP</a>
	 */
	@Nullable
	byte[] dump(byte[] key);

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(byte[])}.
	 *
	 * @param key must not be {@literal null}.
	 * @param ttlInMillis
	 * @param serializedValue must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	default void restore(byte[] key, long ttlInMillis, byte[] serializedValue) {
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
	void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace);

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
	@Nullable
	ValueEncoding encodingOf(byte[] key);

	/**
	 * Get the {@link Duration} since the object stored at the given {@code key} is idle.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/object">Redis Documentation: OBJECT IDLETIME</a>
	 * @since 2.1
	 */
	@Nullable
	Duration idletime(byte[] key);

	/**
	 * Get the number of references of the value associated with the specified {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/object">Redis Documentation: OBJECT REFCOUNT</a>
	 * @since 2.1
	 */
	@Nullable
	Long refcount(byte[] key);

}
