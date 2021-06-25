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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;

/**
 * Hash-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisHashCommands {

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hset">Redis Documentation: HSET</a>
	 */
	@Nullable
	Boolean hSet(byte[] key, byte[] field, byte[] value);

	/**
	 * Set the {@code value} of a hash {@code field} only if {@code field} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hsetnx">Redis Documentation: HSETNX</a>
	 */
	@Nullable
	Boolean hSetNX(byte[] key, byte[] field, byte[] value);

	/**
	 * Get value for given {@code field} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when key or field do not exists or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	@Nullable
	byte[] hGet(byte[] key, byte[] field);

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal empty}.
	 * @return empty {@link List} if key or fields do not exists. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	@Nullable
	List<byte[]> hMGet(byte[] key, byte[]... fields);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code hashes}
	 *
	 * @param key must not be {@literal null}.
	 * @param hashes must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/hmset">Redis Documentation: HMSET</a>
	 */
	void hMSet(byte[] key, Map<byte[], byte[]> hashes);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hincrby">Redis Documentation: HINCRBY</a>
	 */
	@Nullable
	Long hIncrBy(byte[] key, byte[] field, long delta);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hincrbyfloat">Redis Documentation: HINCRBYFLOAT</a>
	 */
	@Nullable
	Double hIncrBy(byte[] key, byte[] field, double delta);

	/**
	 * Determine if given hash {@code field} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hexits">Redis Documentation: HEXISTS</a>
	 */
	@Nullable
	Boolean hExists(byte[] key, byte[] field);

	/**
	 * Delete given hash {@code fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal empty}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	@Nullable
	Long hDel(byte[] key, byte[]... fields);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	@Nullable
	Long hLen(byte[] key);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>?
	 */
	@Nullable
	Set<byte[]> hKeys(byte[] key);

	/**
	 * Get entry set (values) of hash at {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link List} if key does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	@Nullable
	List<byte[]> hVals(byte[] key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link Map} if key does not exist or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	@Nullable
	Map<byte[], byte[]> hGetAll(byte[] key);

	/**
	 * Return a random field from the hash value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	byte[] hRandField(byte[] key);

	/**
	 * Return a random field from the hash value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map.Entry<byte[], byte[]> hRandFieldWithValues(byte[] key);

	/**
	 * Return a random field from the hash value stored at {@code key}. If the provided {@code count} argument is
	 * positive, return a list of distinct fields, capped either at {@code count} or the hash size. If {@code count} is
	 * negative, the behavior changes and the command is allowed to return the same field multiple times. In this case,
	 * the number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	List<byte[]> hRandField(byte[] key, long count);

	/**
	 * Return a random field from the hash value stored at {@code key}. If the provided {@code count} argument is
	 * positive, return a list of distinct fields, capped either at {@code count} or the hash size. If {@code count} is
	 * negative, the behavior changes and the command is allowed to return the same field multiple times. In this case,
	 * the number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	List<Map.Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 */
	Cursor<Map.Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options);

	/**
	 * Returns the length of the value associated with {@code field} in the hash stored at {@code key}. If the key or the
	 * field do not exist, {@code 0} is returned.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	@Nullable
	Long hStrLen(byte[] key, byte[] field);
}
