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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.ObjectUtils;

/**
 * Hash-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tihomir Mateev
 * @author Viktoriya Kutsarova
 * @see RedisCommands
 */
@NullUnmarked
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
	Boolean hSet(byte @NonNull [] key, byte @NonNull [] field, byte @NonNull [] value);

	/**
	 * Set the {@code value} of a hash {@code field} only if {@code field} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hsetnx">Redis Documentation: HSETNX</a>
	 */
	Boolean hSetNX(byte @NonNull [] key, byte @NonNull [] field, byte @NonNull [] value);

	/**
	 * Get value for given {@code field} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when key or field do not exists or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hget">Redis Documentation: HGET</a>
	 */
	byte[] hGet(byte @NonNull [] key, byte @NonNull [] field);

	/**
	 * Get values for given {@code fields} from hash at {@code key}. Values are in the order of the requested keys Absent
	 * field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal empty}.
	 * @return empty {@link List} if key does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 */
	List<byte[]> hMGet(byte @NonNull [] key, byte @NonNull [] @NonNull... fields);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code hashes}
	 *
	 * @param key must not be {@literal null}.
	 * @param hashes must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/hmset">Redis Documentation: HMSET</a>
	 */
	void hMSet(byte @NonNull [] key, Map<byte[], byte[]> hashes);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hincrby">Redis Documentation: HINCRBY</a>
	 */
	Long hIncrBy(byte @NonNull [] key, byte @NonNull [] field, long delta);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hincrbyfloat">Redis Documentation: HINCRBYFLOAT</a>
	 */
	Double hIncrBy(byte @NonNull [] key, byte @NonNull [] field, double delta);

	/**
	 * Determine if given hash {@code field} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hexits">Redis Documentation: HEXISTS</a>
	 */
	Boolean hExists(byte @NonNull [] key, byte @NonNull [] field);

	/**
	 * Delete given hash {@code fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal empty}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 */
	Long hDel(byte @NonNull [] key, byte @NonNull [] @NonNull... fields);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 */
	Long hLen(byte @NonNull [] key);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>?
	 */
	Set<byte @NonNull []> hKeys(byte @NonNull [] key);

	/**
	 * Get entry set (values) of hash at {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link List} if key does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 */
	List<byte @NonNull []> hVals(byte @NonNull [] key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link Map} if key does not exist or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 */
	Map<byte @NonNull [], byte @NonNull []> hGetAll(byte @NonNull [] key);

	/**
	 * Return a random field from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	byte[] hRandField(byte @NonNull [] key);

	/**
	 * Return a random field from the hash along with its value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Map.Entry<byte @NonNull [], byte @NonNull []> hRandFieldWithValues(byte @NonNull [] key);

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
	List<byte @NonNull []> hRandField(byte @NonNull [] key, long count);

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
	List<Map. @NonNull Entry<byte @NonNull [], byte @NonNull []>> hRandFieldWithValues(byte @NonNull [] key, long count);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 */
	Cursor<Map. @NonNull Entry<byte @NonNull [], byte @NonNull []>> hScan(byte @NonNull [] key, ScanOptions options);

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
	Long hStrLen(byte @NonNull [] key, byte @NonNull [] field);

	/**
	 * Apply a given {@link org.springframework.data.redis.core.types.Expiration} to the given {@literal fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration the {@link org.springframework.data.redis.core.types.Expiration} to apply.
	 * @param fields the names of the {@literal fields} to apply the {@literal expiration} to.
	 * @return a {@link List} holding the command result for each field in order - {@code 2} indicating the specific field
	 *         is deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration
	 *         time is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating there is no
	 *         such field;
	 * @since 3.5
	 */
	default List<@NonNull Long> applyHashFieldExpiration(byte @NonNull [] key,
			org.springframework.data.redis.core.types.@NonNull Expiration expiration, byte @NonNull [] @NonNull... fields) {
		return applyHashFieldExpiration(key, expiration, ExpirationOptions.none(), fields);
	}

	/**
	 * @param key must not be {@literal null}.
	 * @param expiration the {@link org.springframework.data.redis.core.types.Expiration} to apply.
	 * @param options additional options to be sent along with the command.
	 * @param fields the names of the {@literal fields} to apply the {@literal expiration} to.
	 * @return a {@link List} holding the command result for each field in order - {@code 2} indicating the specific field
	 *         is deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration
	 *         time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT
	 *         condition is not met); {@code -2} indicating there is no such field;
	 * @since 3.5
	 */
	default List<@NonNull Long> applyHashFieldExpiration(byte @NonNull [] key,
			org.springframework.data.redis.core.types.@NonNull Expiration expiration, @NonNull ExpirationOptions options,
			byte @NonNull [] @NonNull... fields) {

		if (expiration.isPersistent()) {
			return hPersist(key, fields);
		}

		if (ObjectUtils.nullSafeEquals(ExpirationOptions.none(), options)) {
			if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
				if (expiration.isUnixTimestamp()) {
					return hpExpireAt(key, expiration.getExpirationTimeInMilliseconds(), fields);
				}
				return hpExpire(key, expiration.getExpirationTimeInMilliseconds(), fields);
			}
			if (expiration.isUnixTimestamp()) {
				return hExpireAt(key, expiration.getExpirationTimeInSeconds(), fields);
			}
			return hExpire(key, expiration.getExpirationTimeInSeconds(), fields);
		}

		if (ObjectUtils.nullSafeEquals(TimeUnit.MILLISECONDS, expiration.getTimeUnit())) {
			if (expiration.isUnixTimestamp()) {
				return hpExpireAt(key, expiration.getExpirationTimeInMilliseconds(), options.getCondition(), fields);
			}

			return hpExpire(key, expiration.getExpirationTimeInMilliseconds(), options.getCondition(), fields);
		}

		if (expiration.isUnixTimestamp()) {
			return hExpireAt(key, expiration.getExpirationTimeInSeconds(), options.getCondition(), fields);
		}

		return hExpire(key, expiration.getExpirationTimeInSeconds(), options.getCondition(), fields);
	}

	/**
	 * Set time to live for given {@code fields} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds the amount of time after which the fields will be expired in seconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default List<@NonNull Long> hExpire(byte @NonNull [] key, long seconds, byte @NonNull [] @NonNull... fields) {
		return hExpire(key, seconds, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set time to live for given {@code fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param ttl the amount of time after which the fields will be expired in {@link Duration#toSeconds() seconds}
	 *          precision, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default List<@NonNull Long> hExpire(byte @NonNull [] key, @NonNull Duration ttl,
			byte @NonNull [] @NonNull... fields) {
		return hExpire(key, ttl.toSeconds(), fields);
	}

	/**
	 * Set time to live for given {@code fields} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds the amount of time after which the fields will be expired in seconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hExpire(byte @NonNull [] key, long seconds, ExpirationOptions.@NonNull Condition condition,
			byte @NonNull [] @NonNull... fields);

	/**
	 * Set time to live for given {@code fields} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis the amount of time after which the fields will be expired in milliseconds, must not be
	 *          {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set ; {@code -2} indicating there is no
	 *         such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @since 3.5
	 */
	default List<@NonNull Long> hpExpire(byte @NonNull [] key, long millis, byte @NonNull [] @NonNull... fields) {
		return hpExpire(key, millis, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set time to live for given {@code fields} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param ttl the amount of time after which the fields will be expired in {@link Duration#toMillis() milliseconds}
	 *          precision, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @since 3.5
	 */
	default List<@NonNull Long> hpExpire(byte @NonNull [] key, @NonNull Duration ttl,
			byte @NonNull [] @NonNull... fields) {
		return hpExpire(key, ttl.toMillis(), fields);
	}

	/**
	 * Set time to live for given {@code fields} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis the amount of time after which the fields will be expired in milliseconds, must not be
	 *          {@literal null}.
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hpExpire(byte @NonNull [] key, long millis, ExpirationOptions.@NonNull Condition condition,
			byte @NonNull [] @NonNull... fields);

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime the moment in time in which the field expires, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating
	 *         there is no such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIREAT</a>
	 * @since 3.5
	 */
	default List<Long> hExpireAt(byte @NonNull [] key, long unixTime, byte @NonNull [] @NonNull... fields) {
		return hExpireAt(key, unixTime, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime the moment in time in which the field expires, must not be {@literal null}.
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIREAT</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hExpireAt(byte @NonNull [] key, long unixTime, ExpirationOptions.@NonNull Condition condition,
			byte @NonNull [] @NonNull... fields);

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis the moment in time in which the field expires in milliseconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating
	 *         there is no such field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpireat/">Redis Documentation: HPEXPIREAT</a>
	 * @since 3.5
	 */
	default List<@NonNull Long> hpExpireAt(byte @NonNull [] key, long unixTimeInMillis,
			byte @NonNull [] @NonNull... fields) {
		return hpExpireAt(key, unixTimeInMillis, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis the moment in time in which the field expires in milliseconds, must not be {@literal null}.
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpireat/">Redis Documentation: HPEXPIREAT</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hpExpireAt(byte @NonNull [] key, long unixTimeInMillis,
			ExpirationOptions.@NonNull Condition condition, byte @NonNull [] @NonNull... fields);

	/**
	 * Remove the expiration from given {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 1} indicating expiration time is
	 *         removed; {@code -1} field has no expiration time to be removed; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.{@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hPersist(byte @NonNull [] key, byte @NonNull [] @NonNull... fields);

	/**
	 * Get the time to live for {@code fields} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the field exists but has no associated
	 *         expiration time. The command returns {@code -2} if the field does not exist; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hTtl(byte @NonNull [] key, byte @NonNull [] @NonNull... fields);

	/**
	 * Get the time to live for {@code fields} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return for each of the fields supplied - the time to live in the {@link TimeUnit} provided; or a negative value to
	 *         signal an error. The command returns {@code -1} if the key exists but has no associated expiration time.
	 *         The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hTtl(byte @NonNull [] key, @NonNull TimeUnit timeUnit, byte @NonNull [] @NonNull... fields);

	/**
	 * Get the time to live for {@code fields} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in seconds; or a negative
	 *         value to signal an error. The command returns {@code -1} if the key exists but has no associated expiration
	 *         time. The command returns {@code -2} if the key does not exist; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	List<@NonNull Long> hpTtl(byte @NonNull [] key, byte @NonNull [] @NonNull... fields);

    /**
     * Get and delete the value of one or more {@code fields} from hash at {@code key}. Values are returned in the order of
     * the requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     * When the last field is deleted, the key will also be deleted.
     *
     * @param key must not be {@literal null}.
     * @param fields must not be {@literal null}.
     * @return list of values for deleted {@code fields} ({@literal null} for fields that does not exist) or an
	 * empty {@link List} if key does not exist or {@literal null} when used in pipeline / transaction.
     * @see <a href="https://redis.io/commands/hgetdel">Redis Documentation: HGETDEL</a>
     */
    List<byte[]> hGetDel(byte @NonNull [] key, byte @NonNull [] @NonNull... fields);

    /**
     * Get the value of one or more {@code fields} from hash at {@code key} and optionally set expiration time or
     * time-to-live (TTL) for given {@code fields}.
     *
     * @param key must not be {@literal null}.
     * @param fields must not be {@literal null}.
	 * @return list of values for given {@code fields} or an empty {@link List} if key does not
	 * exist or {@literal null} when used in pipeline / transaction.
     * @see <a href="https://redis.io/commands/hgetex">Redis Documentation: HGETEX</a>
     */
    List<byte[]> hGetEx(byte @NonNull [] key, Expiration expiration,
                                byte @NonNull [] @NonNull... fields);

    /**
     * Set field-value pairs in hash at {@literal key} with optional condition and expiration.
     *
     * @param key must not be {@literal null}.
     * @param hashes the field-value pairs to set; must not be {@literal null}.
     * @param hashFieldSetOption the optional condition for setting fields.
     * @param expiration the optional expiration to apply.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hsetex">Redis Documentation: HSETEX</a>
     */
    Boolean hSetEx(byte @NonNull [] key, @NonNull Map<byte[], byte[]> hashes, HashFieldSetOption hashFieldSetOption,
                   Expiration expiration);

    /**
     * {@code HSETEX} command arguments for {@code FNX}, {@code FXX}.
     *
     * @author Viktoriya Kutsarova
     */
    enum HashFieldSetOption {

        /**
         * Do not set any additional command argument.
         */
        UPSERT,

        /**
         * {@code FNX}
         */
        IF_NONE_EXIST,

        /**
         * {@code FXX}
         */
        IF_ALL_EXIST;

        /**
         * Do not set any additional command argument.
         */
        public static HashFieldSetOption upsert() {
            return UPSERT;
        }

        /**
         * {@code FNX}
         */
        public static HashFieldSetOption ifNoneExist() {
            return IF_NONE_EXIST;
        }

        /**
         * {@code FXX}
         */
        public static HashFieldSetOption ifAllExist() {
            return IF_ALL_EXIST;
        }
    }
}
