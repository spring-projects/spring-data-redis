/*
 * Copyright 2011-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.query.SortQuery;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;

/**
 * Interface that specified a basic set of Redis operations, implemented by {@link RedisTemplate}. Not often used but a
 * useful option for extensibility and testability (as it can be easily mocked or stubbed).
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 * @author Mark Paluch
 */
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
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	@Nullable
	<T> T execute(RedisCallback<T> action);

	/**
	 * Executes a Redis session. Allows multiple operations to be executed in the same session enabling 'transactional'
	 * capabilities through {@link #multi()} and {@link #watch(Collection)} operations.
	 *
	 * @param <T> return type
	 * @param session session callback. Must not be {@literal null}.
	 * @return result object returned by the action or <tt>null</tt>
	 */
	@Nullable
	<T> T execute(SessionCallback<T> session);

	/**
	 * Executes the given action object on a pipelined connection, returning the results. Note that the callback
	 * <b>cannot</b> return a non-null value as it gets overwritten by the pipeline. This method will use the default
	 * serializers to deserialize results
	 *
	 * @param action callback object to execute
	 * @return list of objects returned by the pipeline
	 */
	List<Object> executePipelined(RedisCallback<?> action);

	/**
	 * Executes the given action object on a pipelined connection, returning the results using a dedicated serializer.
	 * Note that the callback <b>cannot</b> return a non-null value as it gets overwritten by the pipeline.
	 *
	 * @param action callback object to execute
	 * @param resultSerializer The Serializer to use for individual values or Collections of values. If any returned
	 *          values are hashes, this serializer will be used to deserialize both the key and value
	 * @return list of objects returned by the pipeline
	 */
	List<Object> executePipelined(final RedisCallback<?> action, final RedisSerializer<?> resultSerializer);

	/**
	 * Executes the given Redis session on a pipelined connection. Allows transactions to be pipelined. Note that the
	 * callback <b>cannot</b> return a non-null value as it gets overwritten by the pipeline.
	 *
	 * @param session Session callback
	 * @return list of objects returned by the pipeline
	 */
	List<Object> executePipelined(final SessionCallback<?> session);

	/**
	 * Executes the given Redis session on a pipelined connection, returning the results using a dedicated serializer.
	 * Allows transactions to be pipelined. Note that the callback <b>cannot</b> return a non-null value as it gets
	 * overwritten by the pipeline.
	 *
	 * @param session Session callback
	 * @param resultSerializer
	 * @return list of objects returned by the pipeline
	 */
	List<Object> executePipelined(final SessionCallback<?> session, final RedisSerializer<?> resultSerializer);

	/**
	 * Executes the given {@link RedisScript}
	 *
	 * @param script The script to execute
	 * @param keys Any keys that need to be passed to the script
	 * @param args Any args that need to be passed to the script
	 * @return The return value of the script or null if {@link RedisScript#getResultType()} is null, likely indicating a
	 *         throw-away status reply (i.e. "OK")
	 */
	@Nullable
	<T> T execute(RedisScript<T> script, List<K> keys, Object... args);

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
	@Nullable
	<T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer,
			List<K> keys, Object... args);

	/**
	 * Allocates and binds a new {@link RedisConnection} to the actual return type of the method. It is up to the caller
	 * to free resources after use.
	 *
	 * @param callback must not be {@literal null}.
	 * @return
	 * @since 1.8
	 */
	@Nullable
	<T extends Closeable> T executeWithStickyConnection(RedisCallback<T> callback);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Keys
	// -------------------------------------------------------------------------

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	@Nullable
	Boolean hasKey(K key);

	/**
	 * Count the number of {@code keys} that exist.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys existing among the ones specified as arguments. Keys mentioned multiple times and
	 *         existing are counted multiple times.
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 * @since 2.1
	 */
	@Nullable
	Long countExistingKeys(Collection<K> keys);

	/**
	 * Delete given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal true} if the key was removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	@Nullable
	Boolean delete(K key);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	@Nullable
	Long delete(Collection<K> keys);

	/**
	 * Unlink the {@code key} from the keyspace. Unlike with {@link #delete(Object)} the actual memory reclaiming here
	 * happens asynchronously.
	 *
	 * @param key must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	@Nullable
	Boolean unlink(K key);

	/**
	 * Unlink the {@code keys} from the keyspace. Unlike with {@link #delete(Collection)} the actual memory reclaiming
	 * here happens asynchronously.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/unlink">Redis Documentation: UNLINK</a>
	 * @since 2.1
	 */
	@Nullable
	Long unlink(Collection<K> keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	@Nullable
	DataType type(K key);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	@Nullable
	Set<K> keys(K pattern);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return {@literal null} no keys exist or when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	@Nullable
	K randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	void rename(K oldKey, K newKey);

	/**
	 * Rename key {@code oleName} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	@Nullable
	Boolean renameIfAbsent(K oldKey, K newKey);

	/**
	 * Set time to live for given {@code key}..
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean expire(K key, long timeout, TimeUnit unit);

	/**
	 * Set the expiration for given {@code key} as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param date must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	@Nullable
	Boolean expireAt(K key, Date date);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	@Nullable
	Boolean persist(K key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	@Nullable
	Boolean move(K key, int dbIndex);

	/**
	 * Retrieve serialized version of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/dump">Redis Documentation: DUMP</a>
	 */
	@Nullable
	byte[] dump(K key);

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(Object)}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeToLive
	 * @param unit must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	default void restore(K key, byte[] value, long timeToLive, TimeUnit unit) {
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
	 * @see <a href="http://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	void restore(K key, byte[] value, long timeToLive, TimeUnit unit, boolean replace);


	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	@Nullable
	Long getExpire(K key);

	/**
	 * Get the time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.8
	 */
	@Nullable
	Long getExpire(K key, TimeUnit timeUnit);

	/**
	 * Sort the elements for {@code query}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	List<V> sort(SortQuery<K> query);

	/**
	 * Sort the elements for {@code query} applying {@link RedisSerializer}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	<T> List<T> sort(SortQuery<K> query, RedisSerializer<T> resultSerializer);

	/**
	 * Sort the elements for {@code query} applying {@link BulkMapper}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	<T> List<T> sort(SortQuery<K> query, BulkMapper<T, V> bulkMapper);

	/**
	 * Sort the elements for {@code query} applying {@link BulkMapper} and {@link RedisSerializer}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	<T, S> List<T> sort(SortQuery<K> query, BulkMapper<T, S> bulkMapper, RedisSerializer<S> resultSerializer);

	/**
	 * Sort the elements for {@code query} and store result in {@code storeKey}.
	 *
	 * @param query must not be {@literal null}.
	 * @param storeKey must not be {@literal null}.
	 * @return number of values. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	@Nullable
	Long sort(SortQuery<K> query, K storeKey);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Transactions
	// -------------------------------------------------------------------------

	/**
	 * Watch given {@code key} for modifications during transaction started with {@link #multi()}.
	 *
	 * @param key must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/watch">Redis Documentation: WATCH</a>
	 */
	void watch(K key);

	/**
	 * Watch given {@code keys} for modifications during transaction started with {@link #multi()}.
	 *
	 * @param keys must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/watch">Redis Documentation: WATCH</a>
	 */
	void watch(Collection<K> keys);

	/**
	 * Flushes all the previously {@link #watch(Object)} keys.
	 *
	 * @see <a href="http://redis.io/commands/unwatch">Redis Documentation: UNWATCH</a>
	 */
	void unwatch();

	/**
	 * Mark the start of a transaction block. <br>
	 * Commands will be queued and can then be executed by calling {@link #exec()} or rolled back using {@link #discard()}
	 * <p>
	 *
	 * @see <a href="http://redis.io/commands/multi">Redis Documentation: MULTI</a>
	 */
	void multi();

	/**
	 * Discard all commands issued after {@link #multi()}.
	 *
	 * @see <a href="http://redis.io/commands/discard">Redis Documentation: DISCARD</a>
	 */
	void discard();

	/**
	 * Executes all queued commands in a transaction started with {@link #multi()}. <br>
	 * If used along with {@link #watch(Object)} the operation will fail if any of watched keys has been modified.
	 *
	 * @return List of replies for each executed command.
	 * @see <a href="http://redis.io/commands/exec">Redis Documentation: EXEC</a>
	 */
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
	List<Object> exec(RedisSerializer<?> valueSerializer);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Server Commands
	// -------------------------------------------------------------------------

	/**
	 * Request information and statistics about connected clients.
	 *
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @since 1.3
	 */
	@Nullable
	List<RedisClientInfo> getClientList();

	/**
	 * Closes a given client connection identified by {@literal ip:port} given in {@code client}.
	 *
	 * @param host of connection to close.
	 * @param port of connection to close
	 * @since 1.3
	 */
	void killClient(String host, int port);

	/**
	 * Change redis replication setting to new master.
	 *
	 * @param host must not be {@literal null}.
	 * @param port
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/slaveof">Redis Documentation: SLAVEOF</a>
	 */
	void slaveOf(String host, int port);

	/**
	 * Change server into master.
	 *
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/slaveof">Redis Documentation: SLAVEOF</a>
	 */
	void slaveOfNoOne();

	/**
	 * Publishes the given message to the given channel.
	 *
	 * @param destination the channel to publish to, must not be {@literal null}.
	 * @param message message to publish
	 * @return the number of clients that received the message
	 * @see <a href="http://redis.io/commands/publish">Redis Documentation: PUBLISH</a>
	 */
	void convertAndSend(String destination, Object message);

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
	ClusterOperations<K, V> opsForCluster();

	/**
	 * Returns geospatial specific operations interface.
	 *
	 * @return never {@literal null}.
	 * @since 1.8
	 */
	GeoOperations<K, V> opsForGeo();

	/**
	 * Returns geospatial specific operations interface bound to the given key.
	 *
	 * @param key must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 */
	BoundGeoOperations<K, V> boundGeoOps(K key);

	/**
	 *Returns the operations performed on hash values.
	 *
	 * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @return hash operations
	 */
	<HK, HV> HashOperations<K, HK, HV> opsForHash();

	/**
	 * Returns the operations performed on hash values bound to the given key.
	 * * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @param key Redis key
	 * @return hash operations bound to the given key.
	 */
	<HK, HV> BoundHashOperations<K, HK, HV> boundHashOps(K key);

	/**
	 * @return
	 * @since 1.5
	 */
	HyperLogLogOperations<K, V> opsForHyperLogLog();

	/**
	 * Returns the operations performed on list values.
	 *
	 * @return list operations
	 */
	ListOperations<K, V> opsForList();

	/**
	 * Returns the operations performed on list values bound to the given key.
	 *
	 * @param key Redis key
	 * @return list operations bound to the given key
	 */
	BoundListOperations<K, V> boundListOps(K key);

	/**
	 * Returns the operations performed on set values.
	 *
	 * @return set operations
	 */
	SetOperations<K, V> opsForSet();

	/**
	 * Returns the operations performed on set values bound to the given key.
	 *
	 * @param key Redis key
	 * @return set operations bound to the given key
	 */
	BoundSetOperations<K, V> boundSetOps(K key);

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology).
	 *
	 * @return value operations
	 */
	ValueOperations<K, V> opsForValue();

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology) bound to the given key.
	 *
	 * @param key Redis key
	 * @return value operations bound to the given key
	 */
	BoundValueOperations<K, V> boundValueOps(K key);

	/**
	 * Returns the operations performed on zset values (also known as sorted sets).
	 *
	 * @return zset operations
	 */
	ZSetOperations<K, V> opsForZSet();

	/**
	 * Returns the operations performed on zset values (also known as sorted sets) bound to the given key.
	 *
	 * @param key Redis key
	 * @return zset operations bound to the given key.
	 */
	BoundZSetOperations<K, V> boundZSetOps(K key);

	/**
	 * @return the key {@link RedisSerializer}.
	 */
	RedisSerializer<?> getKeySerializer();

	/**
	 * @return the value {@link RedisSerializer}.
	 */
	RedisSerializer<?> getValueSerializer();

	/**
	 * @return the hash key {@link RedisSerializer}.
	 */
	RedisSerializer<?> getHashKeySerializer();

	/**
	 * @return the hash value {@link RedisSerializer}.
	 */
	RedisSerializer<?> getHashValueSerializer();

}
