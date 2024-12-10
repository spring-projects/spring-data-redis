/*
 * Copyright 2011-2024 the original author or authors.
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
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;

/**
 * Hash-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tihomir Mateev
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
	 * Get values for given {@code fields} from hash at {@code key}. Values are in the order of the requested keys Absent
	 * field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal empty}.
	 * @return empty {@link List} if key does not exist. {@literal null} when used in pipeline / transaction.
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
	 * Return a random field from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	byte[] hRandField(byte[] key);

	/**
	 * Return a random field from the hash along with its value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map.Entry<byte[], byte[]> hRandFieldWithValues(byte[] key);

	/**
	 * Return a random field from the hash stored at {@code key}. If the provided {@code count} argument is positive,
	 * return a list of distinct fields, capped either at {@code count} or the hash size. If {@code count} is negative,
	 * the behavior changes and the command is allowed to return the same field multiple times. In this case, the number
	 * of returned fields is the absolute value of the specified count.
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
	 * Return a random field from the hash along with its value stored at {@code key}. If the provided {@code count}
	 * argument is positive, return a list of distinct fields, capped either at {@code count} or the hash size. If
	 * {@code count} is negative, the behavior changes and the command is allowed to return the same field multiple times.
	 * In this case, the number of returned fields is the absolute value of the specified count.
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
	 * Returns the length of the value associated with {@code field} in the hash stored at {@code key}. If the {@code key}
	 * or the {@code field} do not exist, {@code 0} is returned.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/hstrlen">Redis Documentation: HSTRLEN</a>
	 */
	@Nullable
	Long hStrLen(byte[] key, byte[] field);

	/**
	 * Set time to live for given {@code field} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds the amount of time after which the key will be expired in seconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is deleted
	 *         already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time is set/updated;
	 *         {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not met);
	 *         {@code -2} indicating there is no such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hExpire(byte[] key, long seconds, byte[]... fields);

	/**
	 * Set time to live for given {@code field} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis the amount of time after which the key will be expired in milliseconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is deleted
	 *         already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time is set/updated;
	 *         {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not met);
	 *         {@code -2} indicating there is no such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hpExpire(byte[] key, long millis, byte[]... fields);

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime the moment in time in which the field expires, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is deleted
	 * 	       already due to expiration, or provided expiry interval is in the past; {@code 1} indicating expiration time is
	 * 	       set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not
	 *         met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIREAT</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hExpireAt(byte[] key, long unixTime, byte[]... fields);

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis the moment in time in which the field expires in milliseconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is deleted
	 * 	       already due to expiration, or provided expiry interval is in the past; {@code 1} indicating expiration time is
	 * 	       set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition is not
	 *         met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpireat/">Redis Documentation: HPEXPIREAT</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hpExpireAt(byte[] key, long unixTimeInMillis, byte[]... fields);

	/**
	 * Remove the expiration from given {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 1} indicating expiration time is removed;
	 * 	       {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such field;
	 * 	       {@literal null} when used in pipeline / transaction.{@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hPersist(byte[] key, byte[]... fields);

	/**
	 * Get the time to live for {@code field} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative value
	 *         to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time.
	 * 	       The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hTtl(byte[] key, byte[]... fields);

	/**
	 * Get the time to live for {@code field} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return for each of the fields supplied - the time to live in the {@link TimeUnit} provided; or a negative value
	 *         to signal an error. The command returns {@code -1} if the key exists but has no associated expiration time.
	 * 	       The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.4
	 */
	@Nullable
	List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields);
}
