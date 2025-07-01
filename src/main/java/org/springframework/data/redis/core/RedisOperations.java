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

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.query.SortQuery;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;

/**
 * Interface that specified a basic set of Redis operations, implemented by {@link RedisTemplate}. Not often used but a
 * useful option for extensibility and testability (as it can be easily mocked or stubbed).
 * <p>
 * Redis command methods are exempted from the default {@literal non-nullable} return value assumption as nullness
 * depends not only on the command but also on the connection state. Methods invoked during a transaction or while
 * pipelining are required to return {@literal null} at the time invoking a command as the response is not available
 * until the transaction is executed or the pipeline is closed. To avoid excessive null checks in calling code and to
 * not express a faulty assumption of non-nullness, all command interfaces are annotated with {@code @NullUnmarked}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 * @author ihaohong
 * @author Todd Merrill
 * @author Chen Li
 * @author Vedran Pavic
 * @author Marcin Grzejszczak
 */
@NullUnmarked
public interface RedisOperations<K, V> {

	/**
	 * Executes the given action within a Redis connection. Application exceptions thrown by the action object get
	 * propagated to the caller (can only be unchecked) whenever possible. Redis exceptions are transformed into
	 * appropriate DAO ones. Allows for returning a result object, that is a domain object or a collection of domain
	 * objects. Performs automatic serialization/deserialization for the given objects to and from binary data suitable
	 * for the Redis storage. Note: Callback code is not supposed to handle transactions itself! Use an appropriate
	 * transaction manager. Generally, callback code must not touch any Connection lifecycle methods, like close, to let
	 * the template do its work.
	 *
	 * @param <T> return type
	 * @param action callback object that specifies the Redis action. Must not be {@literal null}.
	 * @return result of the given {@link RedisCallback#doInRedis(RedisConnection)} invocation.
	 */
	<T extends @Nullable Object> T execute(@NonNull RedisCallback<T> action);

	/**
	 * Executes a Redis session. Allows multiple operations to be executed in the same session enabling 'transactional'
	 * capabilities through {@link #multi()} and {@link #watch(Collection)} operations.
	 *
	 * @param <T> return type
	 * @param session session callback. Must not be {@literal null}.
	 * @return result of the given {@link SessionCallback#execute(RedisOperations)} invocation.
	 */
	<T extends @Nullable Object> T execute(@NonNull SessionCallback<T> session);

	/**
	 * Executes the given action object on a pipelined connection, returning the results. Note that the callback
	 * <b>cannot</b> return a non-null value as it gets overwritten by the pipeline. This method will use the default
	 * serializers to deserialize results
	 *
	 * @param action callback object to execute
	 * @return pipeline results of the given {@link RedisCallback#doInRedis(RedisConnection)} invocation. Results are
	 *         collected from {@link RedisConnection} calls, {@link RedisCallback#doInRedis(RedisConnection)} itself must
	 *         return {@literal null}.
	 */
	@NonNull
	List<Object> executePipelined(@NonNull RedisCallback<?> action);

	/**
	 * Executes the given action object on a pipelined connection, returning the results using a dedicated serializer.
	 * Note that the callback <b>cannot</b> return a non-null value as it gets overwritten by the pipeline.
	 *
	 * @param action callback object to execute
	 * @param resultSerializer The Serializer to use for individual values or Collections of values. If any returned
	 *          values are hashes, this serializer will be used to deserialize both the key and value
	 * @return pipeline results of the given {@link RedisCallback#doInRedis(RedisConnection)} invocation. Results are
	 *         collected from {@link RedisConnection} calls, {@link RedisCallback#doInRedis(RedisConnection)} itself must
	 *         return {@literal null}.
	 */
	@NonNull
	List<Object> executePipelined(@NonNull RedisCallback<?> action, @NonNull RedisSerializer<?> resultSerializer);

	/**
	 * Executes the given Redis session on a pipelined connection. Allows transactions to be pipelined. Note that the
	 * callback <b>cannot</b> return a non-null value as it gets overwritten by the pipeline.
	 *
	 * @param session Session callback
	 * @return pipeline results of the given {@link SessionCallback#execute(RedisOperations)} invocation. Results are
	 *         collected from {@link RedisOperations} calls, {@link SessionCallback#execute(RedisOperations)} itself must
	 *         return {@literal null}.
	 */
	@NonNull
	List<Object> executePipelined(@NonNull SessionCallback<?> session);

	/**
	 * Executes the given Redis session on a pipelined connection, returning the results using a dedicated serializer.
	 * Allows transactions to be pipelined. Note that the callback <b>cannot</b> return a non-null value as it gets
	 * overwritten by the pipeline.
	 *
	 * @param session Session callback
	 * @param resultSerializer
	 * @return pipeline results of the given {@link SessionCallback#execute(RedisOperations)} invocation. Results are
	 *         collected from {@link RedisOperations} calls, {@link SessionCallback#execute(RedisOperations)} itself must
	 *         return {@literal null}.
	 */
	@NonNull
	List<Object> executePipelined(@NonNull SessionCallback<?> session, @NonNull RedisSerializer<?> resultSerializer);

	/**
	 * Executes the given {@link RedisScript}
	 *
	 * @param script The script to execute
	 * @param keys Any keys that need to be passed to the script
	 * @param args Any args that need to be passed to the script
	 * @return The return value of the script or null if {@link RedisScript#getResultType()} is null, likely indicating a
	 *         throw-away status reply (i.e. "OK")
	 */
	<T extends @Nullable Object> T execute(@NonNull RedisScript<T> script, @NonNull List<@NonNull K> keys,
			@NonNull Object @NonNull... args);

	/**
	 * Executes the given {@link RedisScript}, using the provided {@link RedisSerializer}s to serialize the script
	 * arguments and result.
	 *
	 * @param script The script to execute
	 * @param argsSerializer The {@link RedisSerializer} to use for serializing args
	 * @param resultSerializer The {@link RedisSerializer} to use for serializing the script return value
	 * @param keys Any keys that need to be passed to the script
	 * @param args Any args that need to be passed to the script
	 * @return The return value of the script or null if {@link RedisScript#getResultType()} is null, likely indicating a
	 *         throw-away status reply (i.e. "OK")
	 */
	<T extends @Nullable Object> T execute(@NonNull RedisScript<T> script, @NonNull RedisSerializer<?> argsSerializer,
			@NonNull RedisSerializer<T> resultSerializer, @NonNull List<@NonNull K> keys, @NonNull Object @NonNull... args);

	/**
	 * Allocates and binds a new {@link RedisConnection} to the actual return type of the method. It is up to the caller
	 * to free resources after use.
	 *
	 * @param callback must not be {@literal null}.
	 * @return the {@link Object result} of the operation performed in the callback or {@literal null}.
	 * @since 1.8
	 */
	<T extends Closeable> T executeWithStickyConnection(@NonNull RedisCallback<T> callback);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Keys
	// -------------------------------------------------------------------------

	/**
	 * Copy given {@code sourceKey} to {@code targetKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param targetKey must not be {@literal null}.
	 * @param replace whether the key was copied. {@literal null} when used in pipeline / transaction.
	 * @return {@code true} when copied successfully or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/copy">Redis Documentation: COPY</a>
	 * @since 2.6
	 */
	Boolean copy(@NonNull K sourceKey, @NonNull K targetKey, boolean replace);

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if key exists. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	Boolean hasKey(@NonNull K key);

	/**
	 * Count the number of {@code keys} that exist.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys existing among the ones specified as arguments. Keys mentioned multiple times and
	 *         existing are counted multiple times.
	 * @see <a href="https://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 * @since 2.1
	 */
	Long countExistingKeys(@NonNull Collection<@NonNull K> keys);

	/**
	 * Delete given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if the key was removed.
	 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Boolean delete(@NonNull K key);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Long delete(@NonNull Collection<@NonNull K> keys);

	/**
	 * Unlink the {@code key} from the keyspace. Unlike with {@link #delete(Object)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param key must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Boolean unlink(@NonNull K key);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #delete(Collection)} the actual memory reclaiming
	 * here happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	Long unlink(@NonNull Collection<@NonNull K> keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	DataType type(@NonNull K key);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	Set<@NonNull K> keys(@NonNull K pattern);

	/**
	 * Use a {@link Cursor} to iterate over keys. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leaks.
	 *
	 * @param options must not be {@literal null}.
	 * @return the result cursor providing access to the scan result. Must be closed once fully processed (e.g. through a
	 *         try-with-resources clause).
	 * @since 2.7
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	@NonNull
	Cursor<@NonNull K> scan(@NonNull ScanOptions options);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return {@literal null} no keys exist or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	K randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	void rename(@NonNull K oldKey, @NonNull K newKey);

	/**
	 * Rename key {@code oldKey} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	Boolean renameIfAbsent(@NonNull K oldKey, @NonNull K newKey);

	/**
	 * Set time to live for given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean expire(@NonNull K key, long timeout, @NonNull TimeUnit unit);

	/**
	 * Set time to live for given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @since 2.3
	 */
	default Boolean expire(@NonNull K key, @NonNull Duration timeout) {

		Assert.notNull(timeout, "Timeout must not be null");

		return TimeoutUtils.hasMillis(timeout) ? expire(key, timeout.toMillis(), TimeUnit.MILLISECONDS)
				: expire(key, timeout.getSeconds(), TimeUnit.SECONDS);
	}

	/**
	 * Set the expiration for given {@code key} as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param date must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean expireAt(@NonNull K key, @NonNull Date date);

	/**
	 * Set the expiration for given {@code key} as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @since 2.3
	 */
	default Boolean expireAt(@NonNull K key, @NonNull Instant expireAt) {

		Assert.notNull(expireAt, "Timestamp must not be null");

		return expireAt(key, Date.from(expireAt));
	}

	/**
	 * Set the expiration for given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return changes to the expiry. {@literal null} when used in pipeline / transaction.
	 * @throws IllegalArgumentException any of the required arguments is {@literal null}.
	 * @see <a href="https://redis.io/commands/expire">Redis Documentation: EXPIRE</a>
	 * @see <a href="https://redis.io/commands/pexpire">Redis Documentation: PEXPIRE</a>
	 * @see <a href="https://redis.io/commands/expireat">Redis Documentation: EXPIREAT</a>
	 * @see <a href="https://redis.io/commands/pexpireat">Redis Documentation: PEXPIREAT</a>
	 * @see <a href="https://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 * @since 3.5
	 */
	ExpireChanges.ExpiryChangeState expire(@NonNull K key, @NonNull Expiration expiration,
			@NonNull ExpirationOptions options);

	/**
	 * Returns a bound operations object to perform expiration operations on the bound key.
	 *
	 * @return the bound operations object to perform operations on the hash field expiration.
	 * @since 3.5
	 */
	default @NonNull BoundKeyExpirationOperations expiration(@NonNull K key) {
		return new DefaultBoundKeyExpirationOperations<>(this, key);
	}

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@code true} when persisted successfully or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	Boolean persist(@NonNull K key);

	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	Long getExpire(@NonNull K key);

	/**
	 * Get the time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.8
	 */
	Long getExpire(@NonNull K key, @NonNull TimeUnit timeUnit);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	Boolean move(@NonNull K key, int dbIndex);

	/**
	 * Retrieve serialized version of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/dump">Redis Documentation: DUMP</a>
	 */
	byte[] dump(@NonNull K key);

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(Object)}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeToLive
	 * @param unit must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	default void restore(@NonNull K key, byte @NonNull [] value, long timeToLive, @NonNull TimeUnit unit) {
		restore(key, value, timeToLive, unit, false);
	}

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(Object)}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeToLive
	 * @param unit must not be {@literal null}.
	 * @param replace use {@literal true} to replace a potentially existing value instead of erroring.
	 * @since 2.1
	 * @see <a href="https://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	void restore(@NonNull K key, byte @NonNull [] value, long timeToLive, @NonNull TimeUnit unit, boolean replace);

	/**
	 * Sort the elements for {@code query}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	List<V> sort(@NonNull SortQuery<@NonNull K> query);

	/**
	 * Sort the elements for {@code query} applying {@link RedisSerializer}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	<T> List<T> sort(@NonNull SortQuery<@NonNull K> query, @NonNull RedisSerializer<T> resultSerializer);

	/**
	 * Sort the elements for {@code query} applying {@link BulkMapper}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	<T> List<T> sort(@NonNull SortQuery<@NonNull K> query, @NonNull BulkMapper<T, V> bulkMapper);

	/**
	 * Sort the elements for {@code query} applying {@link BulkMapper} and {@link RedisSerializer}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	<T, S> List<T> sort(SortQuery<@NonNull K> query, @NonNull BulkMapper<T, S> bulkMapper,
			@NonNull RedisSerializer<S> resultSerializer);

	/**
	 * Sort the elements for {@code query} and store result in {@code storeKey}.
	 *
	 * @param query must not be {@literal null}.
	 * @param storeKey must not be {@literal null}.
	 * @return number of values. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	Long sort(@NonNull SortQuery<@NonNull K> query, @NonNull K storeKey);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Transactions
	// -------------------------------------------------------------------------

	/**
	 * Watch given {@code key} for modifications during transaction started with {@link #multi()}.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/watch">Redis Documentation: WATCH</a>
	 */
	void watch(@NonNull K key);

	/**
	 * Watch given {@code keys} for modifications during transaction started with {@link #multi()}.
	 *
	 * @param keys must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/watch">Redis Documentation: WATCH</a>
	 */
	void watch(@NonNull Collection<@NonNull K> keys);

	/**
	 * Flushes all the previously {@link #watch(Object)} keys.
	 *
	 * @see <a href="https://redis.io/commands/unwatch">Redis Documentation: UNWATCH</a>
	 */
	void unwatch();

	/**
	 * Mark the start of a transaction block. <br>
	 * Commands will be queued and can then be executed by calling {@link #exec()} or rolled back using {@link #discard()}
	 *
	 * @see <a href="https://redis.io/commands/multi">Redis Documentation: MULTI</a>
	 */
	void multi();

	/**
	 * Discard all commands issued after {@link #multi()}.
	 *
	 * @see <a href="https://redis.io/commands/discard">Redis Documentation: DISCARD</a>
	 */
	void discard();

	/**
	 * Executes all queued commands in a transaction started with {@link #multi()}. <br>
	 * If used along with {@link #watch(Object)} the operation will fail if any of watched keys has been modified.
	 *
	 * @return List of replies for each executed command.
	 * @see <a href="https://redis.io/commands/exec">Redis Documentation: EXEC</a>
	 */
	@NonNull
	List<Object> exec();

	/**
	 * Execute a transaction, using the provided {@link RedisSerializer} to deserialize any results that are byte[]s or
	 * Collections of byte[]s. If a result is a Map, the provided {@link RedisSerializer} will be used for both the keys
	 * and values. Other result types (Long, Boolean, etc) are left as-is in the converted results. Tuple results are
	 * automatically converted to TypedTuples.
	 *
	 * @param valueSerializer The {@link RedisSerializer} to use for deserializing the results of transaction exec
	 * @return The deserialized results of transaction exec
	 */
	@NonNull
	List<Object> exec(@NonNull RedisSerializer<?> valueSerializer);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Server Commands
	// -------------------------------------------------------------------------

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @since 1.3
	 */
	List<@NonNull RedisClientInfo> getClientList();

	/**
	 * Closes a given client connection identified by {@literal ip:port} given in {@code client}.
	 *
	 * @param host of connection to close.
	 * @param port of connection to close
	 * @since 1.3
	 */
	void killClient(@NonNull String host, int port);

	/**
	 * Change redis replication setting to new master.
	 *
	 * @param host must not be {@literal null}.
	 * @param port
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/replicaof">Redis Documentation: REPLICAOF</a>
	 */
	void replicaOf(@NonNull String host, int port);

	/**
	 * Change server into master.
	 *
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/replicaof">Redis Documentation: REPLICAOF</a>
	 */
	void replicaOfNoOne();

	/**
	 * Publishes the given message to the given channel.
	 *
	 * @param destination the channel to publish to, must not be {@literal null}.
	 * @param message message to publish.
	 * @return the number of clients that received the message. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/publish">Redis Documentation: PUBLISH</a>
	 */
	Long convertAndSend(@NonNull String destination, @NonNull Object message);

	// -------------------------------------------------------------------------
	// Methods to obtain specific operations interface objects.
	// -------------------------------------------------------------------------

	// operation types

	/**
	 * Returns the cluster specific operations interface.
	 *
	 * @return never {@literal null}.
	 * @since 1.7
	 */
	@NonNull
	ClusterOperations<K, V> opsForCluster();

	/**
	 * Returns geospatial specific operations interface.
	 *
	 * @return never {@literal null}.
	 * @since 1.8
	 */
	@NonNull
	GeoOperations<K, V> opsForGeo();

	/**
	 * Returns geospatial specific operations interface bound to the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 */
	@NonNull
	BoundGeoOperations<K, V> boundGeoOps(@NonNull K key);

	/**
	 * Returns the operations performed on hash values.
	 *
	 * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @return hash operations
	 */
	<HK, HV> @NonNull HashOperations<K, HK, HV> opsForHash();

	/**
	 * Returns the operations performed on hash values bound to the given key.
	 *
	 * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @param key Redis key
	 * @return hash operations bound to the given key.
	 */
	<HK, HV> @NonNull BoundHashOperations<K, HK, HV> boundHashOps(@NonNull K key);

	/**
	 * @return never {@literal null}.
	 * @since 1.5
	 */
	@NonNull
	HyperLogLogOperations<K, V> opsForHyperLogLog();

	/**
	 * Returns the operations performed on list values.
	 *
	 * @return list operations
	 */
	@NonNull
	ListOperations<K, V> opsForList();

	/**
	 * Returns the operations performed on list values bound to the given key.
	 *
	 * @param key Redis key
	 * @return list operations bound to the given key
	 */
	@NonNull
	BoundListOperations<K, V> boundListOps(K key);

	/**
	 * Returns the operations performed on set values.
	 *
	 * @return set operations
	 */
	@NonNull
	SetOperations<K, V> opsForSet();

	/**
	 * Returns the operations performed on set values bound to the given key.
	 *
	 * @param key Redis key
	 * @return set operations bound to the given key
	 */
	@NonNull
	BoundSetOperations<K, V> boundSetOps(@NonNull K key);

	/**
	 * Returns the operations performed on Streams.
	 *
	 * @return stream operations.
	 * @since 2.2
	 */
	<HK, HV> @NonNull StreamOperations<K, HK, HV> opsForStream();

	/**
	 * Returns the operations performed on Streams.
	 *
	 * @param hashMapper the {@link HashMapper} to use when converting {@link ObjectRecord}.
	 * @return stream operations.
	 * @since 2.2
	 */
	<HK, HV> @NonNull StreamOperations<K, HK, HV> opsForStream(
			@NonNull HashMapper<? super K, ? super HK, ? super HV> hashMapper);

	/**
	 * Returns the operations performed on Streams bound to the given key.
	 *
	 * @return stream operations.
	 * @since 2.2
	 */
	<HK, HV> @NonNull BoundStreamOperations<K, HK, HV> boundStreamOps(@NonNull K key);

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology).
	 *
	 * @return value operations
	 */
	@NonNull
	ValueOperations<K, V> opsForValue();

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology) bound to the given key.
	 *
	 * @param key Redis key
	 * @return value operations bound to the given key
	 */
	@NonNull
	BoundValueOperations<K, V> boundValueOps(@NonNull K key);

	/**
	 * Returns the operations performed on zset values (also known as sorted sets).
	 *
	 * @return zset operations
	 */
	@NonNull
	ZSetOperations<K, V> opsForZSet();

	/**
	 * Returns the operations performed on zset values (also known as sorted sets) bound to the given key.
	 *
	 * @param key Redis key
	 * @return zset operations bound to the given key.
	 */
	@NonNull
	BoundZSetOperations<K, V> boundZSetOps(@NonNull K key);

	/**
	 * @return the key {@link RedisSerializer}.
	 */
	@NonNull
	RedisSerializer<?> getKeySerializer();

	/**
	 * @return the value {@link RedisSerializer}.
	 */
	@NonNull
	RedisSerializer<?> getValueSerializer();

	/**
	 * @return the hash key {@link RedisSerializer}.
	 */
	@NonNull
	RedisSerializer<?> getHashKeySerializer();

	/**
	 * @return the hash value {@link RedisSerializer}.
	 */
	@NonNull
	RedisSerializer<?> getHashValueSerializer();

}
