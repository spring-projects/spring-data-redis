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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoGroups;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoStream;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.CollectionUtils;

/**
 * Convenience extension of {@link RedisConnection} that accepts and returns {@link String}s instead of byte arrays.
 * Uses a {@link RedisSerializer} underneath to perform the conversion.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author Tugdual Grall
 * @author Dengliming
 * @author Andrey Shlykov
 * @author ihaohong
 * @author Shyngys Sapraliyev
 * @author Jeonggyu Choi
 * @author Mingi Lee
 * @see RedisCallback
 * @see RedisSerializer
 * @see StringRedisTemplate
 */
@NullUnmarked
public interface StringRedisConnection extends RedisConnection {

	/**
	 * String-friendly ZSet tuple.
	 */
	interface StringTuple extends Tuple {
		String getValueAsString();
	}

	/**
	 * 'Native' or 'raw' execution of the given command along-side the given arguments. The command is executed as is,
	 * with as little 'interpretation' as possible - it is up to the caller to take care of any processing of arguments or
	 * the result.
	 *
	 * @param command Command to execute
	 * @param args Possible command arguments (may be null)
	 * @return execution result.
	 * @see RedisCommands#execute(String, byte[]...)
	 */
	Object execute(@NonNull String command, String... args);

	/**
	 * 'Native' or 'raw' execution of the given command along-side the given arguments. The command is executed as is,
	 * with as little 'interpretation' as possible - it is up to the caller to take care of any processing of arguments or
	 * the result.
	 *
	 * @param command Command to execute
	 * @return execution result.
	 * @see RedisCommands#execute(String, byte[]...)
	 */
	Object execute(@NonNull String command);

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 * @see RedisKeyCommands#exists(byte[])
	 */
	Boolean exists(@NonNull String key);

	/**
	 * Count how many of the given {@code keys} exist.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 * @see RedisKeyCommands#exists(byte[][])
	 * @since 2.1
	 */
	Long exists(@NonNull String @NonNull... keys);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
	 * @see RedisKeyCommands#del(byte[]...)
	 */
	Long del(@NonNull String @NonNull... keys);

	/**
	 * Copy given {@code sourceKey} to {@code targetKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param targetKey must not be {@literal null}.
	 * @param replace whether to replace existing keys.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/copy">Redis Documentation: COPY</a>
	 * @see RedisKeyCommands#copy(byte[], byte[], boolean)
	 */
	Boolean copy(@NonNull String sourceKey, @NonNull String targetKey, boolean replace);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #del(String...)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Long unlink(@NonNull String @NonNull... keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/type">Redis Documentation: TYPE</a>
	 * @see RedisKeyCommands#type(byte[])
	 */
	DataType type(@NonNull String key);

	/**
	 * Alter the last access time of given {@code key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/touch">Redis Documentation: TOUCH</a>
	 * @since 2.1
	 */
	Long touch(@NonNull String @NonNull... keys);

	/**
	 * Retrieve all keys matching the given pattern via {@code KEYS} command.
	 * <p>
	 * <strong>IMPORTANT:</strong> This command is non-interruptible and scans the entire keyspace which may cause
	 * performance issues. Consider {@link #scan(ScanOptions)} for large datasets.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 * @see RedisKeyCommands#keys(byte[])
	 */
	Collection<String> keys(@NonNull String pattern);

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 * @see RedisKeyCommands#rename(byte[], byte[])
	 */
	void rename(@NonNull String oldKey, @NonNull String newKey);

	/**
	 * Rename key {@code oldKey} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 * @see RedisKeyCommands#renameNX(byte[], byte[])
	 */
	Boolean renameNX(@NonNull String oldKey, @NonNull String newKey);

	/**
	 * Set time to live for given {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @return
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @see RedisKeyCommands#expire(byte[], long)
	 */
	default Boolean expire(@NonNull String key, long seconds) {
		return expire(key, seconds, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set time to live for given {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @param seconds
	 * @return
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @see RedisKeyCommands#expire(byte[], long)
	 */
	Boolean expire(@NonNull String key, long seconds, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis
	 * @return
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 * @see RedisKeyCommands#pExpire(byte[], long)
	 */
	default Boolean pExpire(@NonNull String key, long millis) {
		return pExpire(key, millis, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @return
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 * @see RedisKeyCommands#pExpire(byte[], long)
	 */
	Boolean pExpire(@NonNull String key, long millis, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @return
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 * @see RedisKeyCommands#expireAt(byte[], long)
	 */
	default Boolean expireAt(@NonNull String key, long unixTime) {
		return expireAt(key, unixTime, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @return
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 * @see RedisKeyCommands#expireAt(byte[], long)
	 */
	Boolean expireAt(@NonNull String key, long unixTime, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis
	 * @return
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 * @see RedisKeyCommands#pExpireAt(byte[], long)
	 */
	default Boolean pExpireAt(@NonNull String key, long unixTimeInMillis) {
		return pExpireAt(key, unixTimeInMillis, ExpirationOptions.Condition.ALWAYS);
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis
	 * @param condition the condition for expiration, must not be {@literal null}.
	 * @return
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 * @see RedisKeyCommands#pExpireAt(byte[], long)
	 */
	Boolean pExpireAt(@NonNull String key, long unixTimeInMillis, ExpirationOptions.@NonNull Condition condition);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 * @see RedisKeyCommands#persist(byte[])
	 */
	Boolean persist(@NonNull String key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return
	 * @see <a href="https://redis.io/commands/move">Redis Documentation: MOVE</a>
	 * @see RedisKeyCommands#move(byte[], int)
	 */
	Boolean move(@NonNull String key, int dbIndex);

	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 * @see RedisKeyCommands#ttl(byte[])
	 */
	Long ttl(@NonNull String key);

	/**
	 * Get the time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 * @see RedisKeyCommands#ttl(byte[], TimeUnit)
	 */
	Long ttl(@NonNull String key, @NonNull TimeUnit timeUnit);

	/**
	 * Get the precise time to live for {@code key} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 * @see RedisKeyCommands#pTtl(byte[])
	 */
	Long pTtl(@NonNull String key);

	/**
	 * Get the precise time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 * @see RedisKeyCommands#pTtl(byte[], TimeUnit)
	 */
	Long pTtl(@NonNull String key, @NonNull TimeUnit timeUnit);

	/**
	 * Returns {@code message} via server roundtrip.
	 *
	 * @param message the message to echo.
	 * @return
	 * @see <a href="https://redis.io/commands/echo">Redis Documentation: ECHO</a>
	 * @see RedisConnectionCommands#echo(byte[])
	 */
	String echo(@NonNull String message);

	/**
	 * Sort the elements for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param params must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	List<String> sort(@NonNull String key, @NonNull SortParameters params);

	/**
	 * Sort the elements for {@code key} and store result in {@code storeKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param params must not be {@literal null}.
	 * @param storeKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	Long sort(@NonNull String key, @NonNull SortParameters params, @NonNull String storeKey);

	/**
	 * Get the type of internal representation used for storing the value at the given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @since 2.1
	 */
	ValueEncoding encodingOf(@NonNull String key);

	/**
	 * Get the {@link Duration} since the object stored at the given {@code key} is idle.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @since 2.1
	 */
	Duration idletime(@NonNull String key);

	/**
	 * Get the number of references of the value associated with the specified {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @throws IllegalArgumentException if {@code key} is {@literal null}.
	 * @since 2.1
	 */
	Long refcount(@NonNull String key);

	// -------------------------------------------------------------------------
	// Methods dealing with values/Redis strings
	// -------------------------------------------------------------------------

	/**
	 * Get the value of {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/get">Redis Documentation: GET</a>
	 * @see RedisStringCommands#get(byte[])
	 */
	String get(@NonNull String key);

	/**
	 * Return the value at {@code key} and delete the key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getdel">Redis Documentation: GETDEL</a>
	 * @since 2.6
	 */
	String getDel(@NonNull String key);

	/**
	 * Return the value at {@code key} and expire the key by applying {@link Expiration}.
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	String getEx(@NonNull String key, @NonNull Expiration expiration);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 * @see RedisStringCommands#getSet(byte[], byte[])
	 */
	String getSet(@NonNull String key, String value);

	/**
	 * Get multiple {@code keys}. Values are in the order of the requested keys.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/mget">Redis Documentation: MGET</a>
	 * @see RedisStringCommands#mGet(byte[]...)
	 */
	List<String> mGet(@NonNull String @NonNull... keys);

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @see RedisStringCommands#set(byte[], byte[])
	 */
	Boolean set(@NonNull String key, @NonNull String value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration can be {@literal null}. Defaulted to {@link Expiration#persistent()}. Use
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @param option can be {@literal null}. Defaulted to {@link SetOption#UPSERT}.
	 * @since 1.7
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 * @see RedisStringCommands#set(byte[], byte[], Expiration, SetOption)
	 */
	Boolean set(@NonNull String key, @NonNull String value, @Nullable Expiration expiration, @Nullable SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/setnx">Redis Documentation: SETNX</a>
	 * @see RedisStringCommands#setNX(byte[], byte[])
	 */
	Boolean setNX(@NonNull String key, @NonNull String value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 * @see RedisStringCommands#setEx(byte[], long, byte[])
	 */
	Boolean setEx(@NonNull String key, long seconds, @NonNull String value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/psetex">Redis Documentation: PSETEX</a>
	 * @see RedisStringCommands#pSetEx(byte[], long, byte[])
	 */
	Boolean pSetEx(@NonNull String key, long milliseconds, @NonNull String value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param tuple must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/mset">Redis Documentation: MSET</a>
	 * @see RedisStringCommands#mSet(Map)
	 */
	Boolean mSetString(@NonNull Map<@NonNull String, String> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param tuple must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/msetnx">Redis Documentation: MSETNX</a>
	 * @see RedisStringCommands#mSetNX(Map)
	 */
	Boolean mSetNXString(@NonNull Map<@NonNull String, String> tuple);

	/**
	 * Increment an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/incr">Redis Documentation: INCR</a>
	 * @see RedisStringCommands#incr(byte[])
	 */
	Long incr(@NonNull String key);

	/**
	 * Increment an integer value stored of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/incrby">Redis Documentation: INCRBY</a>
	 * @see RedisStringCommands#incrBy(byte[], long)
	 */
	Long incrBy(@NonNull String key, long value);

	/**
	 * Increment a floating point number value of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 * @see RedisStringCommands#incrBy(byte[], double)
	 */
	Double incrBy(@NonNull String key, double value);

	/**
	 * Decrement an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/decr">Redis Documentation: DECR</a>
	 * @see RedisStringCommands#decr(byte[])
	 */
	Long decr(@NonNull String key);

	/**
	 * Decrement an integer value stored as string value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/decrby">Redis Documentation: DECRBY</a>
	 * @see RedisStringCommands#decrBy(byte[], long)
	 */
	Long decrBy(@NonNull String key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/append">Redis Documentation: APPEND</a>
	 * @see RedisStringCommands#append(byte[], byte[])
	 */
	Long append(@NonNull String key, String value);

	/**
	 * Get a substring of value of {@code key} between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 * @see RedisStringCommands#getRange(byte[], long, long)
	 */
	String getRange(@NonNull String key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="https://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 * @see RedisStringCommands#setRange(byte[], byte[], long)
	 */
	void setRange(@NonNull String key, String value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return
	 * @see <a href="https://redis.io/commands/getbit">Redis Documentation: GETBIT</a>
	 * @see RedisStringCommands#getBit(byte[], long)
	 */
	Boolean getBit(@NonNull String key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset}.
	 * @see <a href="https://redis.io/commands/setbit">Redis Documentation: SETBIT</a>
	 * @see RedisStringCommands#setBit(byte[], long, boolean)
	 */
	Boolean setBit(@NonNull String key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/bitcount">Redis Documentation: BITCOUNT</a>
	 * @see RedisStringCommands#bitCount(byte[])
	 */
	Long bitCount(@NonNull String key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code start} and
	 * {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/bitcount">Redis Documentation: BITCOUNT</a>
	 * @see RedisStringCommands#bitCount(byte[], long, long)
	 */
	Long bitCount(@NonNull String key, long start, long end);

	/**
	 * Perform bitwise operations between strings.
	 *
	 * @param op must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/bitop">Redis Documentation: BITOP</a>
	 * @see RedisStringCommands#bitOp(BitOperation, byte[], byte[]...)
	 */
	Long bitOp(@NonNull BitOperation op, @NonNull String destination, @NonNull String @NonNull... keys);

	/**
	 * Return the position of the first bit set to given {@code bit} in a string.
	 *
	 * @param key the key holding the actual String.
	 * @param bit the bit value to look for.
	 * @return {@literal null} when used in pipeline / transaction. The position of the first bit set to 1 or 0 according
	 *         to the request.
	 * @see <a href="https://redis.io/commands/bitpos">Redis Documentation: BITPOS</a>
	 * @since 2.1
	 */
	default Long bitPos(@NonNull String key, boolean bit) {
		return bitPos(key, bit, org.springframework.data.domain.Range.unbounded());
	}

	/**
	 * Return the position of the first bit set to given {@code bit} in a string.
	 * {@link org.springframework.data.domain.Range} start and end can contain negative values in order to index
	 * <strong>bytes</strong> starting from the end of the string, where {@literal -1} is the last byte, {@literal -2} is
	 * the penultimate.
	 *
	 * @param key the key holding the actual String.
	 * @param bit the bit value to look for.
	 * @param range must not be {@literal null}. Use {@link Range#unbounded()} to not limit search.
	 * @return {@literal null} when used in pipeline / transaction. The position of the first bit set to 1 or 0 according
	 *         to the request.
	 * @see <a href="https://redis.io/commands/bitpos">Redis Documentation: BITPOS</a>
	 * @since 2.1
	 */
	Long bitPos(@NonNull String key, boolean bit, org.springframework.data.domain.@NonNull Range<Long> range);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 * @see RedisStringCommands#strLen(byte[])
	 */
	Long strLen(@NonNull String key);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Lists
	// -------------------------------------------------------------------------

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="https://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 * @see RedisListCommands#rPush(byte[], byte[]...)
	 */
	Long rPush(@NonNull String key, @NonNull String @NonNull... values);

	/**
	 * Returns the index of matching elements inside the list stored at given {@literal key}. <br />
	 * Requires Redis 6.0.6 or newer.
	 *
	 * @param key must not be {@literal null}.
	 * @param element must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpos">Redis Documentation: LPOS</a>
	 * @since 2.4
	 */
	default Long lPos(@NonNull String key, @NonNull String element) {
		return CollectionUtils.firstElement(lPos(key, element, null, null));
	}

	/**
	 * Returns the index of matching elements inside the list stored at given {@literal key}. <br />
	 * Requires Redis 6.0.6 or newer.
	 *
	 * @param key must not be {@literal null}.
	 * @param element must not be {@literal null}.
	 * @param rank specifies the "rank" of the first element to return, in case there are multiple matches. A rank of 1
	 *          means to return the first match, 2 to return the second match, and so forth.
	 * @param count number of matches to return.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpos">Redis Documentation: LPOS</a>
	 * @since 2.4
	 */
	List<Long> lPos(@NonNull String key, @NonNull String element, @Nullable Integer rank, @Nullable Integer count);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="https://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 * @see RedisListCommands#lPush(byte[], byte[]...)
	 */
	Long lPush(@NonNull String key, @NonNull String @NonNull... values);

	/**
	 * Append {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/rpushx">Redis Documentation: RPUSHX</a>
	 * @see RedisListCommands#rPushX(byte[], byte[])
	 */
	Long rPushX(@NonNull String key, @NonNull String value);

	/**
	 * Prepend {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/lpushx">Redis Documentation: LPUSHX</a>
	 * @see RedisListCommands#lPushX(byte[], byte[])
	 */
	Long lPushX(@NonNull String key, @NonNull String value);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 * @see RedisListCommands#lLen(byte[])
	 */
	Long lLen(@NonNull String key);

	/**
	 * Get elements between {@code start} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 * @see RedisListCommands#lRange(byte[], long, long)
	 */
	List<String> lRange(@NonNull String key, long start, long end);

	/**
	 * Trim list at {@code key} to elements between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="https://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 * @see RedisListCommands#lTrim(byte[], long, long)
	 */
	void lTrim(@NonNull String key, long start, long end);

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return
	 * @see <a href="https://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 * @see RedisListCommands#lIndex(byte[], long)
	 */
	String lIndex(@NonNull String key, long index);

	/**
	 * Insert {@code value} {@link Position#BEFORE} or {@link Position#AFTER} existing {@code pivot} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param where must not be {@literal null}.
	 * @param pivot
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 * @see RedisListCommands#lIndex(byte[], long)
	 */
	Long lInsert(@NonNull String key, @NonNull Position where, @NonNull String pivot, String value);

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/lmove">Redis Documentation: LMOVE</a>
	 * @see #bLMove(byte[], byte[], Direction, Direction, double)
	 * @see #lMove(byte[], byte[], Direction, Direction)
	 */
	String lMove(@NonNull String sourceKey, @NonNull String destinationKey, @NonNull Direction from,
			@NonNull Direction to);

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param timeout
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/blmove">Redis Documentation: BLMOVE</a>
	 * @see #lMove(byte[], byte[], Direction, Direction)
	 * @see #bLMove(byte[], byte[], Direction, Direction, double)
	 */
	String bLMove(@NonNull String sourceKey, @NonNull String destinationKey, @NonNull Direction from,
			@NonNull Direction to, double timeout);

	/**
	 * Set the {@code value} list element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value
	 * @see <a href="https://redis.io/commands/lset">Redis Documentation: LSET</a>
	 * @see RedisListCommands#lSet(byte[], long, byte[])
	 */
	void lSet(@NonNull String key, long index, String value);

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 * @see RedisListCommands#lRem(byte[], long, byte[])
	 */
	Long lRem(@NonNull String key, long count, String value);

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 * @see RedisListCommands#lPop(byte[])
	 */
	String lPop(@NonNull String key);

	/**
	 * Removes and returns first {@code} elements in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 * @see RedisListCommands#lPop(byte[], long)
	 * @since 2.6
	 */
	List<String> lPop(@NonNull String key, long count);

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 * @see RedisListCommands#rPop(byte[])
	 */
	String rPop(@NonNull String key);

	/**
	 * Removes and returns last {@code} elements in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 * @see RedisListCommands#rPop(byte[], long)
	 * @since 2.6
	 */
	List<String> rPop(@NonNull String key, long count);

	/**
	 * Removes and returns first element from lists stored at {@code keys} (see: {@link #lPop(byte[])}). <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 * @see RedisListCommands#bLPop(int, byte[]...)
	 */
	List<String> bLPop(int timeout, @NonNull String @NonNull... keys);

	/**
	 * Removes and returns last element from lists stored at {@code keys} (see: {@link #rPop(byte[])}). <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 * @see RedisListCommands#bRPop(int, byte[]...)
	 */
	List<String> bRPop(int timeout, @NonNull String @NonNull... keys);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 * @see RedisListCommands#rPopLPush(byte[], byte[])
	 */
	String rPopLPush(@NonNull String srcKey, @NonNull String dstKey);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value (see
	 * {@link #rPopLPush(byte[], byte[])}). <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 * @see RedisListCommands#bRPopLPush(int, byte[], byte[])
	 */
	String bRPopLPush(int timeout, @NonNull String srcKey, @NonNull String dstKey);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Sets
	// -------------------------------------------------------------------------

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="https://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 * @see RedisSetCommands#sAdd(byte[], byte[]...)
	 */
	Long sAdd(@NonNull String key, String... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values
	 * @return
	 * @see <a href="https://redis.io/commands/srem">Redis Documentation: SREM</a>
	 * @see RedisSetCommands#sRem(byte[], byte[]...)
	 */
	Long sRem(@NonNull String key, String... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 * @see RedisSetCommands#sPop(byte[])
	 */
	String sPop(@NonNull String key);

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count the number of random members to return.
	 * @return empty {@link List} if {@literal key} does not exist.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 * @see RedisSetCommands#sPop(byte[], long)
	 * @since 2.0
	 */
	List<String> sPop(@NonNull String key, long count);

	/**
	 * Move {@code value} from {@code srcKey} to {@code destKey}
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 * @see RedisSetCommands#sMove(byte[], byte[], byte[])
	 */
	Boolean sMove(@NonNull String srcKey, @NonNull String destKey, String value);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 * @see RedisSetCommands#sCard(byte[])
	 */
	Long sCard(@NonNull String key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 * @see RedisSetCommands#sIsMember(byte[], byte[])
	 */
	Boolean sIsMember(@NonNull String key, String value);

	/**
	 * Check if set at {@code key} contains one or more {@code values}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/smismember">Redis Documentation: SMISMEMBER</a>
	 * @see RedisSetCommands#sMIsMember(byte[], byte[]...)
	 */
	List<Boolean> sMIsMember(@NonNull String key, String... values);

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 * @see RedisSetCommands#sInter(byte[]...)
	 */
	Set<String> sInter(@NonNull String @NonNull... keys);

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 * @see RedisSetCommands#sInterStore(byte[], byte[]...)
	 */
	Long sInterStore(@NonNull String destKey, @NonNull String @NonNull... keys);

	/**
	 * Returns the cardinality of the set which would result from the intersection of all the given sets.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @see RedisSetCommands#sInterCard(byte[]...)
	 * @since 4.0
	 */
	Long sInterCard(@NonNull String @NonNull... keys);

	/**
	 * Union all sets at given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 * @see RedisSetCommands#sUnion(byte[]...)
	 */
	Set<String> sUnion(String... keys);

	/**
	 * Union all sets at given {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 * @see RedisSetCommands#sUnionStore(byte[], byte[]...)
	 */
	Long sUnionStore(String destKey, String... keys);

	/**
	 * Diff all sets for given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 * @see RedisSetCommands#sDiff(byte[]...)
	 */
	Set<String> sDiff(String... keys);

	/**
	 * Diff all sets for given {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 * @see RedisSetCommands#sDiffStore(byte[], byte[]...)
	 */
	Long sDiffStore(String destKey, String... keys);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 * @see RedisSetCommands#sMembers(byte[])
	 */
	Set<String> sMembers(@NonNull String key);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 * @see RedisSetCommands#sRandMember(byte[])
	 */
	String sRandMember(@NonNull String key);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 * @see RedisSetCommands#sRem(byte[], byte[]...)
	 */
	List<String> sRandMember(@NonNull String key, long count);

	/**
	 * Use a {@link Cursor} to iterate over elements in set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 * @see RedisSetCommands#sScan(byte[], ScanOptions)
	 */
	Cursor<String> sScan(@NonNull String key, ScanOptions options);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Sorted Sets
	// -------------------------------------------------------------------------

	/**
	 * Add {@code value} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param score the score.
	 * @param value the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 * @see RedisZSetCommands#zAdd(byte[], double, byte[])
	 */
	Boolean zAdd(@NonNull String key, double score, String value);

	/**
	 * Add the {@code value} to a sorted set at {@code key}, or update its {@code score} depending on the given
	 * {@link ZAddArgs args}.
	 *
	 * @param key must not be {@literal null}.
	 * @param score must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param args must not be {@literal null} use {@link ZAddArgs#empty()} instead.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 * @see RedisZSetCommands#zAdd(byte[], double, byte[], ZAddArgs)
	 */
	Boolean zAdd(@NonNull String key, double score, String value, ZAddArgs args);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} if it already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples the tuples.
	 * @return
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 * @see RedisZSetCommands#zAdd(byte[], Set)
	 */
	Long zAdd(@NonNull String key, Set<StringTuple> tuples);

	/**
	 * Add {@code tuples} to a sorted set at {@code key}, or update its {@code score} depending on the given
	 * {@link ZAddArgs args}.
	 *
	 * @param key must not be {@literal null}.
	 * @param tuples must not be {@literal null}.
	 * @param args must not be {@literal null} use {@link ZAddArgs#empty()} instead.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zadd">Redis Documentation: ZADD</a>
	 * @see RedisZSetCommands#zAdd(byte[], Set, ZAddArgs)
	 */
	Long zAdd(@NonNull String key, Set<StringTuple> tuples, ZAddArgs args);

	/**
	 * Remove {@code values} from sorted set. Return number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 * @see RedisZSetCommands#zRem(byte[], byte[]...)
	 */
	Long zRem(@NonNull String key, String... values);

	/**
	 * Increment the score of element with {@code value} in sorted set by {@code increment}.
	 *
	 * @param key must not be {@literal null}.
	 * @param increment
	 * @param value the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zincrby">Redis Documentation: ZINCRBY</a>
	 * @see RedisZSetCommands#zIncrBy(byte[], double, byte[])
	 */
	Double zIncrBy(@NonNull String key, double increment, String value);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	String zRandMember(@NonNull String key);

	/**
	 * Get {@code count} random elements from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count if the provided {@code count} argument is positive, return a list of distinct fields, capped either at
	 *          {@code count} or the set size. If {@code count} is negative, the behavior changes and the command is
	 *          allowed to return the same value multiple times. In this case, the number of returned values is the
	 *          absolute value of the specified count.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	List<String> zRandMember(@NonNull String key, long count);

	/**
	 * Get random element from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	StringTuple zRandMemberWithScore(@NonNull String key);

	/**
	 * Get {@code count} random elements from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count if the provided {@code count} argument is positive, return a list of distinct fields, capped either at
	 *          {@code count} or the set size. If {@code count} is negative, the behavior changes and the command is
	 *          allowed to return the same value multiple times. In this case, the number of returned values is the
	 *          absolute value of the specified count.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zrandmember">Redis Documentation: ZRANDMEMBER</a>
	 */
	List<StringTuple> zRandMemberWithScores(@NonNull String key, long count);

	/**
	 * Determine the index of element with {@code value} in a sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zrank">Redis Documentation: ZRANK</a>
	 * @see RedisZSetCommands#zRank(byte[], byte[])
	 */
	Long zRank(@NonNull String key, String value);

	/**
	 * Determine the index of element with {@code value} in a sorted set when scored high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrank">Redis Documentation: ZREVRANK</a>
	 * @see RedisZSetCommands#zRevRank(byte[], byte[])
	 */
	Long zRevRank(@NonNull String key, String value);

	/**
	 * Get elements between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 * @see RedisZSetCommands#zRange(byte[], long, long)
	 */
	Set<String> zRange(@NonNull String key, long start, long end);

	/**
	 * Get set of {@link Tuple}s between {@code start} and {@code end} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/zrange">Redis Documentation: ZRANGE</a>
	 * @see RedisZSetCommands#zRangeWithScores(byte[], long, long)
	 */
	Set<StringTuple> zRangeWithScores(@NonNull String key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRangeByScore(byte[], double, double)
	 */
	Set<String> zRangeByScore(@NonNull String key, double min, double max);

	/**
	 * Get set of {@link Tuple}s where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double)
	 */
	Set<StringTuple> zRangeByScoreWithScores(@NonNull String key, double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRangeByScore(byte[], double, double, long, long)
	 */
	Set<String> zRangeByScore(@NonNull String key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set.
	 *
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRangeByScoreWithScores(byte[], double, double, long, long)
	 */
	Set<StringTuple> zRangeByScoreWithScores(@NonNull String key, double min, double max, long offset, long count);

	/**
	 * Get elements in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 * @see RedisZSetCommands#zRevRange(byte[], long, long)
	 */
	Set<String> zRevRange(@NonNull String key, long start, long end);

	/**
	 * Get set of {@link Tuple}s in range from {@code start} to {@code end} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 * @see RedisZSetCommands#zRevRangeWithScores(byte[], long, long)
	 */
	Set<StringTuple> zRevRangeWithScores(@NonNull String key, long start, long end);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set ordered from high to low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrange">Redis Documentation: ZREVRANGE</a>
	 * @see RedisZSetCommands#zRevRangeByScore(byte[], double, double)
	 */
	Set<String> zRevRangeByScore(@NonNull String key, double min, double max);

	/**
	 * Get set of {@link Tuple} where score is between {@code min} and {@code max} from sorted set ordered from high to
	 * low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double)
	 */
	Set<StringTuple> zRevRangeByScoreWithScores(@NonNull String key, double min, double max);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRevRangeByScore(byte[], double, double, long, long)
	 */
	Set<String> zRevRangeByScore(@NonNull String key, double min, double max, long offset, long count);

	/**
	 * Get set of {@link Tuple} in range from {@code start} to {@code end} where score is between {@code min} and
	 * {@code max} from sorted set ordered high -> low.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 * @see <a href="https://redis.io/commands/zrevrangebyscore">Redis Documentation: ZREVRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRevRangeByScoreWithScores(byte[], double, double, long, long)
	 */
	Set<StringTuple> zRevRangeByScoreWithScores(@NonNull String key, double min, double max, long offset, long count);

	/**
	 * Count number of elements within sorted set with scores between {@code min} and {@code max}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="https://redis.io/commands/zcount">Redis Documentation: ZCOUNT</a>
	 * @see RedisZSetCommands#zCount(byte[], double, double)
	 */
	Long zCount(@NonNull String key, double min, double max);

	/**
	 * Count number of elements within sorted set with value between {@code Range#min} and {@code Range#max} applying
	 * lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zlexcount">Redis Documentation: ZLEXCOUNT</a>
	 * @see RedisZSetCommands#zLexCount(byte[], org.springframework.data.domain.Range)
	 */
	Long zLexCount(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Tuple zPopMin(@NonNull String key);

	/**
	 * Remove and return {@code count} values with their score having the lowest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmin">Redis Documentation: ZPOPMIN</a>
	 * @since 2.6
	 */
	Set<StringTuple> zPopMin(@NonNull String key, long count);

	/**
	 * Remove and return the value with its score having the lowest score from sorted set at {@code key}. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/bzpopmin">Redis Documentation: BZPOPMIN</a>
	 * @since 2.6
	 */
	StringTuple bZPopMin(@NonNull String key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	StringTuple zPopMax(@NonNull String key);

	/**
	 * Remove and return {@code count} values with their score having the highest score from sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of elements to pop.
	 * @return {@literal null} when the sorted set is empty or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zpopmax">Redis Documentation: ZPOPMAX</a>
	 * @since 2.6
	 */
	Set<StringTuple> zPopMax(@NonNull String key, long count);

	/**
	 * Remove and return the value with its score having the highest score from sorted set at {@code key}. <b>Blocks
	 * connection</b> until element available or {@code timeout} reached.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/bzpopmax">Redis Documentation: BZPOPMAX</a>
	 * @since 2.6
	 */
	StringTuple bZPopMax(@NonNull String key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Get the size of sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zcard">Redis Documentation: ZCARD</a>
	 * @see RedisZSetCommands#zCard(byte[])
	 */
	Long zCard(@NonNull String key);

	/**
	 * Get the score of element with {@code value} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value the value.
	 * @return
	 * @see <a href="https://redis.io/commands/zscore">Redis Documentation: ZSCORE</a>
	 * @see RedisZSetCommands#zScore(byte[], byte[])
	 */
	Double zScore(@NonNull String key, String value);

	/**
	 * Get the scores of elements with {@code values} from sorted set with key {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values the values.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/zmscore">Redis Documentation: ZMSCORE</a>
	 * @see RedisZSetCommands#zMScore(byte[], byte[][])
	 * @since 2.6
	 */
	List<Double> zMScore(@NonNull String key, String... values);

	/**
	 * Remove elements in range between {@code start} and {@code end} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyrank">Redis Documentation: ZREMRANGEBYRANK</a>
	 * @see RedisZSetCommands#zRemRange(byte[], long, long)
	 */
	Long zRemRange(@NonNull String key, long start, long end);

	/**
	 * Remove all elements between the lexicographical {@link Range}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return the number of elements removed, or {@literal null} when used in pipeline / transaction.
	 * @since 2.5
	 * @see <a href="https://redis.io/commands/zremrangebylex">Redis Documentation: ZREMRANGEBYLEX</a>
	 */
	Long zRemRangeByLex(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range);

	/**
	 * Remove elements with scores between {@code min} and {@code max} from sorted set with {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param min
	 * @param max
	 * @return
	 * @see <a href="https://redis.io/commands/zremrangebyscore">Redis Documentation: ZREMRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRemRangeByScore(byte[], double, double)
	 */
	Long zRemRangeByScore(@NonNull String key, double min, double max);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<String> zDiff(@NonNull String @NonNull... sets);

	/**
	 * Diff sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiff">Redis Documentation: ZDIFF</a>
	 */
	Set<StringTuple> zDiffWithScores(@NonNull String @NonNull... sets);

	/**
	 * Diff sorted {@code sets} and store result in destination {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zdiffstore">Redis Documentation: ZDIFFSTORE</a>
	 */
	Long zDiffStore(@NonNull String destKey, @NonNull String @NonNull... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<String> zInter(@NonNull String @NonNull... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<StringTuple> zInterWithScores(@NonNull String @NonNull... sets);

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	default Set<StringTuple> zInterWithScores(@NonNull Aggregate aggregate, int @NonNull [] weights,
			@NonNull String @NonNull... sets) {
		return zInterWithScores(aggregate, Weights.of(weights), sets);
	}

	/**
	 * Intersect sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zinter">Redis Documentation: ZINTER</a>
	 */
	Set<StringTuple> zInterWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			@NonNull String @NonNull... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 * @see RedisZSetCommands#zInterStore(byte[], byte[]...)
	 */
	Long zInterStore(@NonNull String destKey, @NonNull String @NonNull... sets);

	/**
	 * Intersect sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zinterstore">Redis Documentation: ZINTERSTORE</a>
	 * @see RedisZSetCommands#zInterStore(byte[], Aggregate, int[], byte[]...)
	 */
	Long zInterStore(@NonNull String destKey, @NonNull Aggregate aggregate, int @NonNull [] weights,
			@NonNull String @NonNull... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<String> zUnion(@NonNull String @NonNull... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<StringTuple> zUnionWithScores(@NonNull String @NonNull... sets);

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	default Set<StringTuple> zUnionWithScores(@NonNull Aggregate aggregate, int @NonNull [] weights,
			@NonNull String @NonNull... sets) {
		return zUnionWithScores(aggregate, Weights.of(weights), sets);
	}

	/**
	 * Union sorted {@code sets}.
	 *
	 * @param aggregate must not be {@literal null}.
	 * @param weights must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/zunion">Redis Documentation: ZUNION</a>
	 */
	Set<StringTuple> zUnionWithScores(@NonNull Aggregate aggregate, @NonNull Weights weights,
			@NonNull String @NonNull... sets);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 * @see RedisZSetCommands#zUnionStore(byte[], byte[]...)
	 */
	Long zUnionStore(@NonNull String destKey, @NonNull String @NonNull... sets);

	/**
	 * Union sorted {@code sets} and store result in destination {@code key}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param aggregate must not be {@literal null}.
	 * @param weights
	 * @param sets must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/zunionstore">Redis Documentation: ZUNIONSTORE</a>
	 * @see RedisZSetCommands#zUnionStore(byte[], Aggregate, int[], byte[]...)
	 */
	Long zUnionStore(@NonNull String destKey, @NonNull Aggregate aggregate, int @NonNull [] weights,
			@NonNull String @NonNull... sets);

	/**
	 * Use a {@link Cursor} to iterate over elements in sorted set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options can be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/zscan">Redis Documentation: ZSCAN</a>
	 * @see RedisZSetCommands#zScan(byte[], ScanOptions)
	 */
	Cursor<StringTuple> zScan(@NonNull String key, ScanOptions options);

	/**
	 * Get elements where score is between {@code min} and {@code max} from sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min must not be {@literal null}.
	 * @param max must not be {@literal null}.
	 * @return
	 * @since 1.5
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRangeByScore(byte[], String, String)
	 */
	Set<String> zRangeByScore(@NonNull String key, @NonNull String min, @NonNull String max);

	/**
	 * Get elements in range from {@code start} to {@code end} where score is between {@code min} and {@code max} from
	 * sorted set.
	 *
	 * @param key must not be {@literal null}.
	 * @param min must not be {@literal null}.
	 * @param max must not be {@literal null}.
	 * @param offset
	 * @param count
	 * @return
	 * @since 1.5
	 * @see <a href="https://redis.io/commands/zrangebyscore">Redis Documentation: ZRANGEBYSCORE</a>
	 * @see RedisZSetCommands#zRangeByScore(byte[], double, double, long, long)
	 */
	Set<String> zRangeByScore(@NonNull String key, @NonNull String min, @NonNull String max, long offset, long count);

	/**
	 * Get all the elements in the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 * @see RedisZSetCommands#zRangeByLex(byte[])
	 */
	Set<String> zRangeByLex(@NonNull String key);

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 * @see RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.domain.Range)
	 */
	Set<String> zRangeByLex(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range);

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in lexicographical ordering. Result is
	 * limited via {@link org.springframework.data.redis.connection.Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @since 1.6
	 * @see <a href="https://redis.io/commands/zrangebylex">Redis Documentation: ZRANGEBYLEX</a>
	 * @see RedisZSetCommands#zRangeByLex(byte[], org.springframework.data.domain.Range,
	 *      org.springframework.data.redis.connection.Limit)
	 */
	Set<String> zRangeByLex(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * Get all the elements in the sorted set at {@literal key} in reversed lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 * @see RedisZSetCommands#zRevRangeByLex(byte[])
	 */
	default Set<String> zRevRangeByLex(@NonNull String key) {
		return zRevRangeByLex(key, org.springframework.data.domain.Range.unbounded());
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in reversed lexicographical ordering.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 * @see RedisZSetCommands#zRevRangeByLex(byte[], org.springframework.data.domain.Range)
	 */
	default Set<String> zRevRangeByLex(@NonNull String key,
			org.springframework.data.domain.@NonNull Range<String> range) {
		return zRevRangeByLex(key, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * Get all the elements in {@link Range} from the sorted set at {@literal key} in reversed lexicographical ordering.
	 * Result is limited via {@link org.springframework.data.redis.connection.Limit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/zrevrangebylex">Redis Documentation: ZREVRANGEBYLEX</a>
	 * @see RedisZSetCommands#zRevRangeByLex(byte[], org.springframework.data.domain.Range,
	 *      org.springframework.data.redis.connection.Limit)
	 */
	Set<String> zRevRangeByLex(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * This command is like ZRANGE , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long zRangeStoreByLex(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<String> range) {
		return zRangeStoreByLex(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long zRangeStoreByLex(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<String> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * This command is like ZRANGE … REV , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long zRangeStoreRevByLex(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<String> range) {
		return zRangeStoreRevByLex(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE … REV , but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long zRangeStoreRevByLex(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<String> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * This command is like ZRANGE, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long zRangeStoreByScore(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range) {
		return zRangeStoreByScore(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long zRangeStoreByScore(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * This command is like ZRANGE … REV, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	default Long zRangeStoreRevByScore(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range) {
		return zRangeStoreRevByScore(dstKey, srcKey, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * This command is like ZRANGE … REV, but stores the result in the {@literal dstKey} destination key.
	 *
	 * @param dstKey must not be {@literal null}.
	 * @param srcKey must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.0
	 * @see <a href="https://redis.io/commands/zrangestore">Redis Documentation: ZRANGESTORE</a>
	 */
	Long zRangeStoreRevByScore(@NonNull String dstKey, @NonNull String srcKey,
			org.springframework.data.domain.@NonNull Range<? extends Number> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Hashes
	// -------------------------------------------------------------------------

	/**
	 * Set the {@code value} of a hash {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/hset">Redis Documentation: HSET</a>
	 * @see RedisHashCommands#hSet(byte[], byte[], byte[])
	 */
	Boolean hSet(@NonNull String key, @NonNull String field, String value);

	/**
	 * Set the {@code value} of a hash {@code field} only if {@code field} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="https://redis.io/commands/hsetnx">Redis Documentation: HSETNX</a>
	 * @see RedisHashCommands#hSetNX(byte[], byte[], byte[])
	 */
	Boolean hSetNX(@NonNull String key, @NonNull String field, String value);

	/**
	 * Get value for given {@code field} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hget">Redis Documentation: HGET</a>
	 * @see RedisHashCommands#hGet(byte[], byte[])
	 */
	String hGet(@NonNull String key, @NonNull String field);

	/**
	 * Get values for given {@code fields} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hmget">Redis Documentation: HMGET</a>
	 * @see RedisHashCommands#hMGet(byte[], byte[]...)
	 */
	List<String> hMGet(@NonNull String key, @NonNull String @NonNull... fields);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code hashes}
	 *
	 * @param key must not be {@literal null}.
	 * @param hashes must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/hmset">Redis Documentation: HMSET</a>
	 * @see RedisHashCommands#hMGet(byte[], byte[]...)
	 */
	void hMSet(@NonNull String key, @NonNull Map<@NonNull String, String> hashes);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param delta
	 * @return
	 * @see <a href="https://redis.io/commands/hincrby">Redis Documentation: HINCRBY</a>
	 * @see RedisHashCommands#hIncrBy(byte[], byte[], long)
	 */
	Long hIncrBy(@NonNull String key, @NonNull String field, long delta);

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field
	 * @param delta
	 * @return
	 * @see <a href="https://redis.io/commands/hincrbyfloat">Redis Documentation: HINCRBYFLOAT</a>
	 * @see RedisHashCommands#hIncrBy(byte[], byte[], double)
	 */
	Double hIncrBy(@NonNull String key, @NonNull String field, double delta);

	/**
	 * Return a random field from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	String hRandField(@NonNull String key);

	/**
	 * Return a random field from the hash along with its value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Map.@Nullable Entry<String, String> hRandFieldWithValues(@NonNull String key);

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
	List<String> hRandField(@NonNull String key, long count);

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
	List<Map.Entry<String, String>> hRandFieldWithValues(@NonNull String key, long count);

	/**
	 * Determine if given hash {@code field} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hexits">Redis Documentation: HEXISTS</a>
	 * @see RedisHashCommands#hExists(byte[], byte[])
	 */
	Boolean hExists(@NonNull String key, @NonNull String field);

	/**
	 * Delete given hash {@code fields}.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hdel">Redis Documentation: HDEL</a>
	 * @see RedisHashCommands#hDel(byte[], byte[]...)
	 */
	Long hDel(@NonNull String key, @NonNull String @NonNull... fields);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hlen">Redis Documentation: HLEN</a>
	 * @see RedisHashCommands#hLen(byte[])
	 */
	Long hLen(@NonNull String key);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hkeys">Redis Documentation: HKEYS</a>?
	 * @see RedisHashCommands#hKeys(byte[])
	 */
	Set<String> hKeys(@NonNull String key);

	/**
	 * Get entry set (values) of hash at {@code field}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hvals">Redis Documentation: HVALS</a>
	 * @see RedisHashCommands#hVals(byte[])
	 */
	List<String> hVals(@NonNull String key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/hgetall">Redis Documentation: HGETALL</a>
	 * @see RedisHashCommands#hGetAll(byte[])
	 */
	Map<String, String> hGetAll(@NonNull String key);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 * @see RedisHashCommands#hScan(byte[], ScanOptions)
	 */
	Cursor<Map.Entry<String, String>> hScan(@NonNull String key, ScanOptions options);

	/**
	 * Returns the length of the value associated with {@code field} in the hash stored at {@code key}. If the key or the
	 * field do not exist, {@code 0} is returned.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	Long hStrLen(@NonNull String key, @NonNull String field);

	/**
	 * Set time to live for given {@code field} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds the amount of time after which the key will be expired in seconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	default List<Long> hExpire(@NonNull String key, long seconds, @NonNull String @NonNull... fields) {
		return hExpire(key, seconds, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set time to live for given {@code field} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds the amount of time after which the key will be expired in seconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	List<Long> hExpire(@NonNull String key, long seconds, ExpirationOptions.@NonNull Condition condition,
			@NonNull String @NonNull... fields);

	/**
	 * Set time to live for given {@code field} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis the amount of time after which the key will be expired in milliseconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set; {@code -2} indicating there is no such
	 *         field; {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @since 3.5
	 */
	default List<Long> hpExpire(@NonNull String key, long millis, @NonNull String @NonNull... fields) {
		return hpExpire(key, millis, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set time to live for given {@code field} in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param millis the amount of time after which the key will be expired in milliseconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is 0; {@code 1} indicating expiration time
	 *         is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX | GT | LT condition
	 *         is not met); {@code -2} indicating there is no such field; {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpire/">Redis Documentation: HPEXPIRE</a>
	 * @since 3.5
	 */
	List<Long> hpExpire(@NonNull String key, long millis, ExpirationOptions.@NonNull Condition condition,
			@NonNull String @NonNull... fields);

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
	default List<Long> hExpireAt(@NonNull String key, long unixTime, @NonNull String @NonNull... fields) {
		return hExpireAt(key, unixTime, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTime the moment in time in which the field expires, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIREAT</a>
	 * @since 3.5
	 */
	List<Long> hExpireAt(@NonNull String key, long unixTime, ExpirationOptions.@NonNull Condition condition,
			@NonNull String @NonNull... fields);

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
	default List<Long> hpExpireAt(@NonNull String key, long unixTimeInMillis, @NonNull String @NonNull... fields) {
		return hpExpireAt(key, unixTimeInMillis, ExpirationOptions.Condition.ALWAYS, fields);
	}

	/**
	 * Set the expiration for given {@code field} as a {@literal UNIX} timestamp in milliseconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param unixTimeInMillis the moment in time in which the field expires in milliseconds, must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: {@code 2} indicating the specific field is
	 *         deleted already due to expiration, or provided expiry interval is in the past; {@code 1} indicating
	 *         expiration time is set/updated; {@code 0} indicating the expiration time is not set (a provided NX | XX |
	 *         GT | LT condition is not met); {@code -2} indicating there is no such field; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hpexpireat/">Redis Documentation: HPEXPIREAT</a>
	 * @since 3.5
	 */
	List<Long> hpExpireAt(@NonNull String key, long unixTimeInMillis, ExpirationOptions.@NonNull Condition condition,
			@NonNull String @NonNull... fields);

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
	List<Long> hPersist(@NonNull String key, @NonNull String @NonNull... fields);

	/**
	 * Get the time to live for {@code fields} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in milliseconds; or a
	 *         negative value to signal an error. The command returns {@code -1} if the key exists but has no associated
	 *         expiration time. The command returns {@code -2} if the key does not exist; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	List<Long> hTtl(@NonNull String key, @NonNull String @NonNull... fields);

	/**
	 * Get the time to live for {@code fields} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in the {@link TimeUnit}
	 *         provided; or a negative value to signal an error. The command returns {@code -1} if the key exists but has
	 *         no associated expiration time. The command returns {@code -2} if the key does not exist; {@literal null}
	 *         when used in pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	List<Long> hTtl(@NonNull String key, @NonNull TimeUnit timeUnit, @NonNull String @NonNull... fields);

	/**
	 * Get the time to live for {@code fields} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return a list of {@link Long} values for each of the fields provided: the time to live in milliseconds; or a
	 *         negative value to signal an error. The command returns {@code -1} if the key exists but has no associated
	 *         expiration time. The command returns {@code -2} if the key does not exist; {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	List<Long> hpTtl(@NonNull String key, @NonNull String @NonNull... fields);

    /**
     * Get and delete the value of one or more {@code fields} from hash at {@code key}. When the last field is deleted,
     * the key will also be deleted.
     *
     * @param key must not be {@literal null}.
     * @param fields must not be {@literal null}.
     * @return empty {@link List} if key does not exist. {@literal null} when used in pipeline / transaction.
     * @see <a href="https://redis.io/commands/hmget">Redis Documentation: HMGET</a>
     * @see RedisHashCommands#hMGet(byte[], byte[]...)
     */
    List<String> hGetDel(@NonNull String key, @NonNull String @NonNull... fields);

    /**
     * Get the value of one or more {@code fields} from hash at {@code key} and optionally set expiration time or
     * time-to-live (TTL) for given {@code fields}.
     *
     * @param key must not be {@literal null}.
     * @param fields must not be {@literal null}.
     * @return empty {@link List} if key does not exist. {@literal null} when used in pipeline / transaction.
     * @see <a href="https://redis.io/commands/hgetex">Redis Documentation: HGETEX</a>
     * @see RedisHashCommands#hGetEx(byte[], Expiration, byte[]...)
     */
    List<String> hGetEx(@NonNull String key, Expiration expiration, @NonNull String @NonNull... fields);

    /**
     * Set field-value pairs in hash at {@literal key} with optional condition and expiration.
     *
     * @param key must not be {@literal null}.
     * @param hashes the field-value pairs to set; must not be {@literal null}.
     * @param condition the optional condition for setting fields.
     * @param expiration the optional expiration to apply.
     * @return never {@literal null}.
     * @see <a href="https://redis.io/commands/hsetex">Redis Documentation: HSETEX</a>
     * @see RedisHashCommands#hSetEx(byte[], Map, HashFieldSetOption, Expiration)
     */
    Boolean hSetEx(@NonNull String key, @NonNull Map<@NonNull String, String> hashes, HashFieldSetOption condition,
                   Expiration expiration);

	// -------------------------------------------------------------------------
	// Methods dealing with HyperLogLog
	// -------------------------------------------------------------------------

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @since 1.5
	 * @see <a href="https://redis.io/commands/pfadd">Redis Documentation: PFADD</a>
	 * @see RedisHyperLogLogCommands#pfAdd(byte[], byte[]...)
	 */
	Long pfAdd(@NonNull String key, String... values);

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/pfcount">Redis Documentation: PFCOUNT</a>
	 * @see RedisHyperLogLogCommands#pfCount(byte[]...)
	 */
	Long pfCount(@NonNull String @NonNull... keys);

	/**
	 * Merge N different HyperLogLogs at {@literal sourceKeys} into a single {@literal destinationKey}.
	 *
	 * @param destinationKey must not be {@literal null}.
	 * @param sourceKeys must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/pfmerge">Redis Documentation: PFMERGE</a>
	 * @see RedisHyperLogLogCommands#pfMerge(byte[], byte[]...)
	 */
	void pfMerge(@NonNull String destinationKey, @NonNull String @NonNull... sourceKeys);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Geo-Indexes
	// -------------------------------------------------------------------------

	/**
	 * Add {@link Point} with given member {@literal name} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param point must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @return Number of elements added.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @see RedisGeoCommands#geoAdd(byte[], Point, byte[])
	 */
	Long geoAdd(@NonNull String key, @NonNull Point point, @NonNull String member);

	/**
	 * Add {@link GeoLocation} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param location must not be {@literal null}.
	 * @return Number of elements added.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @see RedisGeoCommands#geoAdd(byte[], GeoLocation)
	 */
	Long geoAdd(@NonNull String key, @NonNull GeoLocation<String> location);

	/**
	 * Add {@link Map} of member / {@link Point} pairs to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param memberCoordinateMap must not be {@literal null}.
	 * @return Number of elements added.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @see RedisGeoCommands#geoAdd(byte[], Map)
	 */
	Long geoAdd(@NonNull String key, @NonNull Map<@NonNull String, @NonNull Point> memberCoordinateMap);

	/**
	 * Add {@link GeoLocation}s to {@literal key}
	 *
	 * @param key must not be {@literal null}.
	 * @param locations must not be {@literal null}.
	 * @return Number of elements added.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geoadd">Redis Documentation: GEOADD</a>
	 * @see RedisGeoCommands#geoAdd(byte[], Iterable)
	 */
	Long geoAdd(@NonNull String key, @NonNull Iterable<@NonNull GeoLocation<String>> locations);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 * @see RedisGeoCommands#geoDist(byte[], byte[], byte[])
	 */
	Distance geoDist(@NonNull String key, @NonNull String member1, @NonNull String member2);

	/**
	 * Get the {@link Distance} between {@literal member1} and {@literal member2} in the given {@link Metric}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member1 must not be {@literal null}.
	 * @param member2 must not be {@literal null}.
	 * @param metric must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geodist">Redis Documentation: GEODIST</a>
	 * @see RedisGeoCommands#geoDist(byte[], byte[], byte[], Metric)
	 */
	Distance geoDist(@NonNull String key, @NonNull String member1, @NonNull String member2, @NonNull Metric metric);

	/**
	 * Get geohash representation of the position for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geohash">Redis Documentation: GEOHASH</a>
	 * @see RedisGeoCommands#geoHash(byte[], byte[]...)
	 */
	List<String> geoHash(@NonNull String key, @NonNull String @NonNull... members);

	/**
	 * Get the {@link Point} representation of positions for one or more {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/geopos">Redis Documentation: GEOPOS</a>
	 * @see RedisGeoCommands#geoPos(byte[], byte[]...)
	 */
	List<Point> geoPos(@NonNull String key, @NonNull String @NonNull... members);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 * @see RedisGeoCommands#geoRadius(byte[], Circle)
	 */
	GeoResults<GeoLocation<String>> geoRadius(@NonNull String key, @NonNull Circle within);

	/**
	 * Get the {@literal member}s within the boundaries of a given {@link Circle} applying {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param within must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/georadius">Redis Documentation: GEORADIUS</a>
	 * @see RedisGeoCommands#geoRadius(byte[], Circle, GeoRadiusCommandArgs)
	 */
	GeoResults<GeoLocation<String>> geoRadius(@NonNull String key, @NonNull Circle within,
			@NonNull GeoRadiusCommandArgs args);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@literal radius}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 * @see RedisGeoCommands#geoRadiusByMember(byte[], byte[], double)
	 */
	GeoResults<GeoLocation<String>> geoRadiusByMember(@NonNull String key, @NonNull String member, double radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@link Distance}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 * @see RedisGeoCommands#geoRadiusByMember(byte[], byte[], Distance)
	 */
	GeoResults<GeoLocation<String>> geoRadiusByMember(@NonNull String key, @NonNull String member,
			@NonNull Distance radius);

	/**
	 * Get the {@literal member}s within the circle defined by the {@literal members} coordinates and given
	 * {@link Distance} and {@link GeoRadiusCommandArgs}.
	 *
	 * @param key must not be {@literal null}.
	 * @param member must not be {@literal null}.
	 * @param radius must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 * @see <a href="https://redis.io/commands/georadiusbymember">Redis Documentation: GEORADIUSBYMEMBER</a>
	 * @see RedisGeoCommands#geoRadiusByMember(byte[], byte[], Distance, GeoRadiusCommandArgs)
	 */
	GeoResults<GeoLocation<String>> geoRadiusByMember(@NonNull String key, @NonNull String member,
			@NonNull Distance radius, @NonNull GeoRadiusCommandArgs args);

	/**
	 * Remove the {@literal member}s.
	 *
	 * @param key must not be {@literal null}.
	 * @param members must not be {@literal null}.
	 * @since 1.8
	 * @return Number of members elements removed.
	 * @see <a href="https://redis.io/commands/zrem">Redis Documentation: ZREM</a>
	 * @see RedisGeoCommands#geoRemove(byte[], byte[]...)
	 */
	Long geoRemove(@NonNull String key, @NonNull String @NonNull... members);

	/**
	 * Return the members of a geo set which are within the borders of the area specified by a given {@link GeoShape
	 * shape}. The query's center point is provided by {@link GeoReference}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param predicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	GeoResults<GeoLocation<String>> geoSearch(@NonNull String key, @NonNull GeoReference<String> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchCommandArgs args);

	/**
	 * Query the members of a geo set which are within the borders of the area specified by a given {@link GeoShape shape}
	 * and store the result at {@code destKey}. The query's center point is provided by {@link GeoReference}.
	 *
	 * @param key must not be {@literal null}.
	 * @param reference must not be {@literal null}.
	 * @param predicate must not be {@literal null}.
	 * @param args must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/geosearch">Redis Documentation: GEOSEARCH</a>
	 */
	Long geoSearchStore(String destKey, @NonNull String key, @NonNull GeoReference<String> reference,
			@NonNull GeoShape predicate, @NonNull GeoSearchStoreCommandArgs args);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Pub/Sub
	// -------------------------------------------------------------------------

	/**
	 * Publishes the given message to the given channel.
	 *
	 * @param channel the channel to publish to, must not be {@literal null}.
	 * @param message message to publish
	 * @return the number of clients that received the message
	 * @see <a href="https://redis.io/commands/publish">Redis Documentation: PUBLISH</a>
	 * @see RedisPubSubCommands#publish(byte[], byte[])
	 */
	Long publish(@NonNull String channel, @NonNull String message);

	/**
	 * Subscribes the connection to the given channels. Once subscribed, a connection enters listening mode and can only
	 * subscribe to other channels or unsubscribe. No other commands are accepted until the connection is unsubscribed.
	 * <p>
	 * Note that this operation is blocking and the current thread starts waiting for new messages immediately.
	 *
	 * @param listener message listener, must not be {@literal null}.
	 * @param channels channel names, must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/subscribe">Redis Documentation: SUBSCRIBE</a>
	 * @see RedisPubSubCommands#subscribe(MessageListener, byte[]...)
	 */
	void subscribe(@NonNull MessageListener listener, @NonNull String @NonNull... channels);

	/**
	 * Subscribes the connection to all channels matching the given patterns. Once subscribed, a connection enters
	 * listening mode and can only subscribe to other channels or unsubscribe. No other commands are accepted until the
	 * connection is unsubscribed.
	 * <p>
	 * Note that this operation is blocking and the current thread starts waiting for new messages immediately.
	 *
	 * @param listener message listener, must not be {@literal null}.
	 * @param patterns channel name patterns, must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/psubscribe">Redis Documentation: PSUBSCRIBE</a>
	 * @see RedisPubSubCommands#pSubscribe(MessageListener, byte[]...)
	 */
	void pSubscribe(@NonNull MessageListener listener, @NonNull String @NonNull... patterns);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Lua Scripting
	// -------------------------------------------------------------------------

	/**
	 * Load lua script into scripts cache, without executing it.<br>
	 * Execute the script by calling {@link #evalSha(byte[], ReturnType, int, byte[]...)}.
	 *
	 * @param script must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/script-load">Redis Documentation: SCRIPT LOAD</a>
	 * @see RedisScriptingCommands#scriptLoad(byte[])
	 */
	String scriptLoad(@NonNull String script);

	/**
	 * Evaluate given {@code script}.
	 *
	 * @param script must not be {@literal null}.
	 * @param returnType must not be {@literal null}.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/eval">Redis Documentation: EVAL</a>
	 * @see RedisScriptingCommands#eval(byte[], ReturnType, int, byte[]...)
	 */
	<T> T eval(@NonNull String script, @NonNull ReturnType returnType, int numKeys,
			@NonNull String @NonNull... keysAndArgs);

	/**
	 * Evaluate given {@code scriptSha}.
	 *
	 * @param scriptSha must not be {@literal null}.
	 * @param returnType must not be {@literal null}.
	 * @param numKeys
	 * @param keysAndArgs must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/evalsha">Redis Documentation: EVALSHA</a>
	 * @see RedisScriptingCommands#evalSha(String, ReturnType, int, byte[]...)
	 */
	<T> T evalSha(@NonNull String scriptSha, @NonNull ReturnType returnType, int numKeys,
			@NonNull String @NonNull... keysAndArgs);

	/**
	 * Assign given name to current connection.
	 *
	 * @param name
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/client-setname">Redis Documentation: CLIENT SETNAME</a>
	 * @see RedisServerCommands#setClientName(byte[])
	 */
	void setClientName(@NonNull String name);

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/client-list">Redis Documentation: CLIENT LIST</a>
	 * @see RedisServerCommands#getClientList()
	 */
	List<RedisClientInfo> getClientList();

	/**
	 * Get / Manipulate specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset stored
	 * at a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param command must not be {@literal null}.
	 * @return
	 */
	List<Long> bitfield(@NonNull String key, @NonNull BitFieldSubCommands command);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Streams
	// -------------------------------------------------------------------------

	static RecordId[] entryIds(String... entryIds) {

		if (entryIds.length == 1) {
			return new RecordId[] { RecordId.of(entryIds[0]) };
		}

		return Arrays.stream(entryIds).map(RecordId::of).toArray(RecordId[]::new);
	}

	/**
	 * Acknowledge one or more record as processed.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @param entryIds record Id's to acknowledge.
	 * @return length of acknowledged records. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xack">Redis Documentation: XACK</a>
	 */
	default Long xAck(@NonNull String key, @NonNull String group, @NonNull String @NonNull... entryIds) {
		return xAck(key, group, entryIds(entryIds));
	}

	Long xAck(@NonNull String key, @NonNull String group, @NonNull RecordId @NonNull... recordIds);

	/**
	 * Append a record to the stream {@code key}.
	 *
	 * @param key the stream key.
	 * @param body record body.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xadd">Redis Documentation: XADD</a>
	 */
	default RecordId xAdd(@NonNull String key, @NonNull Map<@NonNull String, String> body) {
		return xAdd(StreamRecords.newRecord().in(key).ofStrings(body));
	}

	/**
	 * Append the given {@link StringRecord} to the stream stored at {@link StringRecord#getStream()}.
	 *
	 * @param record must not be {@literal null}.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 */
	default RecordId xAdd(@NonNull StringRecord record) {
		return xAdd(record, XAddOptions.none());
	}

	/**
	 * Append the given {@link StringRecord} to the stream stored at {@link StringRecord#getStream()}.
	 *
	 * @param record must not be {@literal null}.
	 * @param options must not be {@literal null}, use {@link XAddOptions#none()} instead.
	 * @return the record Id. {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	RecordId xAdd(@NonNull StringRecord record, @NonNull XAddOptions options);

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer} without increasing the delivered
	 * count.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param options must not be {@literal null}.
	 * @return list of {@link RecordId ids} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	List<RecordId> xClaimJustId(@NonNull String key, @NonNull String group, @NonNull String newOwner,
			@NonNull XClaimOptions options);

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param minIdleTime must not be {@literal null}.
	 * @param recordIds must not be {@literal null}.
	 * @return list of {@link StringRecord} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	default List<StringRecord> xClaim(@NonNull String key, @NonNull String group, @NonNull String newOwner,
			@NonNull Duration minIdleTime, @NonNull RecordId @NonNull... recordIds) {
		return xClaim(key, group, newOwner, XClaimOptions.minIdle(minIdleTime).ids(recordIds));
	}

	/**
	 * Change the ownership of a pending message to the given new {@literal consumer}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param group the name of the {@literal consumer group}.
	 * @param newOwner the name of the new {@literal consumer}.
	 * @param options must not be {@literal null}.
	 * @return list of {@link StringRecord} that changed user.
	 * @see <a href="https://redis.io/commands/xclaim">Redis Documentation: XCLAIM</a>
	 * @since 2.3
	 */
	List<StringRecord> xClaim(@NonNull String key, @NonNull String group, @NonNull String newOwner,
			@NonNull XClaimOptions options);

	/**
	 * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the
	 * number of IDs passed in case certain IDs do not exist.
	 *
	 * @param key the stream key.
	 * @param entryIds stream record Id's.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xdel">Redis Documentation: XDEL</a>
	 */
	default Long xDel(@NonNull String key, @NonNull String @NonNull... entryIds) {
		return xDel(key, entryIds(entryIds));
	}

	Long xDel(@NonNull String key, @NonNull RecordId @NonNull... recordIds);

	/**
	 * Create a consumer group.
	 *
	 * @param key the stream key.
	 * @param readOffset
	 * @param group name of the consumer group.
	 * @since 2.2
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	String xGroupCreate(@NonNull String key, @NonNull ReadOffset readOffset, @NonNull String group);

	/**
	 * Create a consumer group.
	 *
	 * @param key the stream key.
	 * @param readOffset
	 * @param group name of the consumer group.
	 * @param mkStream if true the group will create the stream if needed (MKSTREAM)
	 * @since
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	String xGroupCreate(@NonNull String key, @NonNull ReadOffset readOffset, @NonNull String group, boolean mkStream);

	/**
	 * Delete a consumer from a consumer group.
	 *
	 * @param key the stream key.
	 * @param consumer consumer identified by group name and consumer key.
	 * @since 2.2
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 */
	Boolean xGroupDelConsumer(@NonNull String key, @NonNull Consumer consumer);

	/**
	 * Destroy a consumer group.
	 *
	 * @param key the stream key.
	 * @param group name of the consumer group.
	 * @return {@literal true} if successful. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 */
	Boolean xGroupDestroy(@NonNull String key, String group);

	/**
	 * Obtain general information about the stream stored at the specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoStream xInfo(@NonNull String key);

	/**
	 * Obtain information about {@literal consumer groups} associated with the stream stored at the specified
	 * {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoGroups xInfoGroups(@NonNull String key);

	/**
	 * Obtain information about every consumer in a specific {@literal consumer group} for the stream stored at the
	 * specified {@literal key}.
	 *
	 * @param key the {@literal key} the stream is stored at.
	 * @param groupName name of the {@literal consumer group}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.3
	 */
	XInfoConsumers xInfoConsumers(@NonNull String key, @NonNull String groupName);

	/**
	 * Get the length of a stream.
	 *
	 * @param key the stream key.
	 * @return length of the stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xlen">Redis Documentation: XLEN</a>
	 */
	Long xLen(@NonNull String key);

	/**
	 * Obtain the {@link PendingMessagesSummary} for a given {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @return a summary of pending messages within the given {@literal consumer group} or {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessagesSummary xPending(@NonNull String key, @NonNull String groupName);

	// /**
	// * Obtained detailed information about all pending messages for a given {@link Consumer}.
	// *
	// * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	// * @param consumer the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	// * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	// * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	// * @since 3.5
	// */
	// @Nullable
	// default PendingMessages xPending(String key, Consumer consumer) {
	// return xPending(key, consumer.getGroup(), consumer.getName());
	// }

	// /**
	// * Obtained detailed information about all pending messages for a given {@literal consumer}.
	// *
	// * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	// * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	// * @param consumerName the consumer to fetch {@link PendingMessages} for. Must not be {@literal null}.
	// * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	// * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	// * @since 3.5
	// */
	// @Nullable
	// default PendingMessages xPending(String key, String groupName, String consumerName) {
	// return xPending(key, groupName, XPendingOptions.unbounded().consumer(consumerName));
	// }

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given
	 * {@link org.springframework.data.domain.Range} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param consumerName the name of the {@literal consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages xPending(@NonNull String key, @NonNull String groupName, @NonNull String consumerName,
			org.springframework.data.domain.@NonNull Range<String> range, @NonNull Long count);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given
	 * {@link org.springframework.data.domain.Range} and {@literal consumer} within a {@literal consumer group} and over a
	 * given {@link Duration} of idle time.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param consumerName the name of the {@literal consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param minIdleTime the minimum idle time to filter pending messages. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer} in given {@literal consumer group} or {@literal null}
	 *         when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	PendingMessages xPending(@NonNull String key, @NonNull String groupName, @NonNull String consumerName,
			org.springframework.data.domain.@NonNull Range<String> range, @NonNull Long count, @NonNull Duration minIdleTime);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given
	 * {@link org.springframework.data.domain.Range} and {@link Consumer} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	default PendingMessages xPending(@NonNull String key, @NonNull Consumer consumer, org.springframework.data.domain.@NonNull Range<String> range,
		@NonNull Long count) {
		return xPending(key, consumer.getGroup(), consumer.getName(), range, count);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given
	 * {@link org.springframework.data.domain.Range} and {@link Consumer} within a {@literal consumer group} and over a
	 * given {@link Duration} of idle time.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param consumer the name of the {@link Consumer}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param minIdleTime the minimum idle time to filter pending messages. Must not be {@literal null}.
	 * @return pending messages for the given {@link Consumer} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	default PendingMessages xPending(@NonNull String key, @NonNull Consumer consumer, org.springframework.data.domain.@NonNull Range<String> range,
		@NonNull Long count, @NonNull Duration minIdleTime) {
		return xPending(key, consumer.getGroup(), consumer.getName(), range, count, minIdleTime);
	}

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given
	 * {@link org.springframework.data.domain.Range} within a {@literal consumer group}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages xPending(@NonNull String key, @NonNull String groupName,
			org.springframework.data.domain.@NonNull Range<String> range, @NonNull Long count);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} for a given
	 * {@link org.springframework.data.domain.Range} within a {@literal consumer group} and over a given {@link Duration}
	 * of idle time.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param range the range of messages ids to search within. Must not be {@literal null}.
	 * @param count limit the number of results. Must not be {@literal null}.
	 * @param minIdleTime the minimum idle time to filter pending messages. Must not be {@literal null}.
	 * @return pending messages for the given {@literal consumer group} or {@literal null} when used in pipeline /
	 *         transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 4.0
	 */
	PendingMessages xPending(@NonNull String key, @NonNull String groupName, org.springframework.data.domain.@NonNull Range<String> range,
		@NonNull Long count, @NonNull Duration minIdleTime);

	/**
	 * Obtain detailed information about pending {@link PendingMessage messages} applying given {@link XPendingOptions
	 * options}.
	 *
	 * @param key the {@literal key} the stream is stored at. Must not be {@literal null}.
	 * @param groupName the name of the {@literal consumer group}. Must not be {@literal null}.
	 * @param options the options containing {@literal range}, {@literal consumer} and {@literal count}. Must not be
	 *          {@literal null}.
	 * @return pending messages matching given criteria or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/xpending">Redis Documentation: xpending</a>
	 * @since 2.3
	 */
	PendingMessages xPending(@NonNull String key, @NonNull String groupName, @NonNull XPendingOptions options);

	/**
	 * Read records from a stream within a specific {@link Range}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	default List<StringRecord> xRange(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range) {
		return xRange(key, range, org.springframework.data.redis.connection.Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a
	 * {@link org.springframework.data.redis.connection.Limit}.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xrange">Redis Documentation: XRANGE</a>
	 */
	List<StringRecord> xRange(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param stream the streams to read from.
	 * @return list ith members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default List<StringRecord> xReadAsString(@NonNull StreamOffset<String> stream) {
		return xReadAsString(StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default List<StringRecord> xReadAsString(@NonNull StreamOffset<String>... streams) {
		return xReadAsString(StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	default List<StringRecord> xReadAsString(@NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<String> stream) {
		return xReadAsString(readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s.
	 *
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xread">Redis Documentation: XREAD</a>
	 */
	List<StringRecord> xReadAsString(@NonNull StreamReadOptions readOptions, @NonNull StreamOffset<String>... streams);

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default List<StringRecord> xReadGroupAsString(@NonNull Consumer consumer, @NonNull StreamOffset<String> stream) {
		return xReadGroupAsString(consumer, StreamReadOptions.empty(), new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default List<StringRecord> xReadGroupAsString(@NonNull Consumer consumer,
			@NonNull StreamOffset<String> @NonNull... streams) {
		return xReadGroupAsString(consumer, StreamReadOptions.empty(), streams);
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param stream the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	default List<StringRecord> xReadGroupAsString(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<String> stream) {
		return xReadGroupAsString(consumer, readOptions, new StreamOffset[] { stream });
	}

	/**
	 * Read records from one or more {@link StreamOffset}s using a consumer group.
	 *
	 * @param consumer consumer/group.
	 * @param readOptions read arguments.
	 * @param streams the streams to read from.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xreadgroup">Redis Documentation: XREADGROUP</a>
	 */
	List<StringRecord> xReadGroupAsString(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<String> @NonNull... streams);

	/**
	 * Read records from a stream within a specific {@link Range} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	default List<StringRecord> xRevRange(@NonNull String key,
			org.springframework.data.domain.@NonNull Range<String> range) {
		return xRevRange(key, range, Limit.unlimited());
	}

	/**
	 * Read records from a stream within a specific {@link Range} applying a
	 * {@link org.springframework.data.redis.connection.Limit} in reverse order.
	 *
	 * @param key the stream key.
	 * @param range must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @return list with members of the resulting stream. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xrevrange">Redis Documentation: XREVRANGE</a>
	 */
	List<StringRecord> xRevRange(@NonNull String key, org.springframework.data.domain.@NonNull Range<String> range,
			org.springframework.data.redis.connection.@NonNull Limit limit);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @since 2.2
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long xTrim(@NonNull String key, long count);

	/**
	 * Trims the stream to {@code count} elements.
	 *
	 * @param key the stream key.
	 * @param count length of the stream.
	 * @param approximateTrimming the trimming must be performed in a approximated way in order to maximize performances.
	 * @return number of removed entries. {@literal null} when used in pipeline / transaction.
	 * @since 2.4
	 * @see <a href="https://redis.io/commands/xtrim">Redis Documentation: XTRIM</a>
	 */
	Long xTrim(@NonNull String key, long count, boolean approximateTrimming);
}
