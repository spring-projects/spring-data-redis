/*
 * Copyright 2011-2015 the original author or authors.
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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.query.SortQuery;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Interface that specified a basic set of Redis operations, implemented by {@link RedisTemplate}. Not often used but a
 * useful option for extensibility and testability (as it can be easily mocked or stubbed).
 * 
 * @author Costin Leau
 * @author Christoph Strobl
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
	 * @param action callback object that specifies the Redis action
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(RedisCallback<T> action);

	/**
	 * Executes a Redis session. Allows multiple operations to be executed in the same session enabling 'transactional'
	 * capabilities through {@link #multi()} and {@link #watch(Collection)} operations.
	 * 
	 * @param <T> return type
	 * @param session session callback
	 * @return result object returned by the action or <tt>null</tt>
	 */
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
	<T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer,
			List<K> keys, Object... args);

	Boolean hasKey(K key);

	void delete(K key);

	void delete(Collection<K> key);

	DataType type(K key);

	Set<K> keys(K pattern);

	K randomKey();

	void rename(K oldKey, K newKey);

	Boolean renameIfAbsent(K oldKey, K newKey);

	Boolean expire(K key, long timeout, TimeUnit unit);

	Boolean expireAt(K key, Date date);

	Boolean persist(K key);

	Boolean move(K key, int dbIndex);

	byte[] dump(K key);

	void restore(K key, byte[] value, long timeToLive, TimeUnit unit);

	Long getExpire(K key);

	Long getExpire(K key, TimeUnit timeUnit);

	void watch(K keys);

	void watch(Collection<K> keys);

	void unwatch();

	/**
	 * '
	 */
	void multi();

	void discard();

	List<Object> exec();

	/**
	 * /** Request information and statistics about connected clients.
	 * 
	 * @return {@link List} of {@link RedisClientInfo} objects.
	 * @since 1.3
	 */
	List<RedisClientInfo> getClientList();

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

	// pubsub functionality on the template
	void convertAndSend(String destination, Object message);

	// operation types
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
	 * Returns the operations performed on zset values (also known as sorted sets).
	 * 
	 * @return zset operations
	 */
	ZSetOperations<K, V> opsForZSet();

	/**
	 * @return
	 * @since 1.5
	 */
	HyperLogLogOperations<K, V> opsForHyperLogLog();

	/**
	 * Returns the operations performed on zset values (also known as sorted sets) bound to the given key.
	 * 
	 * @param key Redis key
	 * @return zset operations bound to the given key.
	 */
	BoundZSetOperations<K, V> boundZSetOps(K key);

	/**
	 * Returns the operations performed on hash values.
	 * 
	 * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @return hash operations
	 */
	<HK, HV> HashOperations<K, HK, HV> opsForHash();

	/**
	 * Returns the operations performed on hash values bound to the given key.
	 * 
	 * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @param key Redis key
	 * @return hash operations bound to the given key.
	 */
	<HK, HV> BoundHashOperations<K, HK, HV> boundHashOps(K key);

	/**
	 * Returns the cluster specific operations interface.
	 * 
	 * @return never {@literal null}.
	 * @since 1.7
	 */
	ClusterOperations<K, V> opsForCluster();

	List<V> sort(SortQuery<K> query);

	<T> List<T> sort(SortQuery<K> query, RedisSerializer<T> resultSerializer);

	<T> List<T> sort(SortQuery<K> query, BulkMapper<T, V> bulkMapper);

	<T, S> List<T> sort(SortQuery<K> query, BulkMapper<T, S> bulkMapper, RedisSerializer<S> resultSerializer);

	Long sort(SortQuery<K> query, K storeKey);

	RedisSerializer<?> getValueSerializer();

	RedisSerializer<?> getKeySerializer();

	RedisSerializer<?> getHashKeySerializer();

	RedisSerializer<?> getHashValueSerializer();

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
	 * @param host
	 * @param port
	 * @since 1.3
	 */
	void slaveOf(String host, int port);

	/**
	 * Change server into master.
	 * 
	 * @since 1.3
	 */
	void slaveOfNoOne();
}
