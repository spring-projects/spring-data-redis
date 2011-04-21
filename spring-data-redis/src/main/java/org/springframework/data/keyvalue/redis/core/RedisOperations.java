/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.core.query.SortQuery;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;


/**
 * Interface that specified a basic set of Redis operations, implemented by {@link RedisTemplate}.
 * Not often used but a useful option for extensibility and testability (as it can be easily mocked or stubbed). 
 * 
 * @author Costin Leau
 */
public interface RedisOperations<K, V> {

	/**
	 * Executes the given action within a Redis connection.
	 * 
	 * Application exceptions thrown by the action object get propagated to the caller (can only be unchecked) whenever possible.
	 * Redis exceptions are transformed into appropriate DAO ones. 
	 * Allows for returning a result object, that is a domain object or a collection of domain objects.
	 * Performs automatic serialization/deserialization for the given objects to and from binary data suitable for the Redis storage.   
	 * 
	 * Note: Callback code is not supposed to handle transactions itself! Use an appropriate transaction manager. 
	 * Generally, callback code must not touch any Connection lifecycle methods, like close, to let the template do its work. 
	 * 
	 * @param <T> return type
	 * @param action callback object that specifies the Redis action
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(RedisCallback<T> action);


	/**
	 * Executes a Redis session.
	 * 
	 * Allows multiple operations to be executed in the same session enabling 'transactional' capabilities through {@link #multi()} 
	 * and {@link #watch(Collection)} operations.
	 *  
	 * @param <T> return type
	 * @param session session callback
	 * @return result object returned by the action or <tt>null</tt>
	 */
	<T> T execute(SessionCallback<T> session);

	//	/**
	//	 * Executes the given action object on a pipelined connection, returning the results. Note that the callback <b>cannot</b>
	//	 * return a non-null value as it gets overwritten by the pipeline.
	//	 * 
	//	 * @param <T> list element return type
	//	 * @param action callback object to execute 
	//	 * @return list of objects returned by the pipeline
	//	 */
	//	List<V> executePipelined(RedisCallback<?> action);


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

	Long getExpire(K key);

	void watch(K keys);

	void watch(Collection<K> keys);

	void unwatch();

	/**'
	 * 
	 */
	void multi();

	void discard();

	List<Object> exec();

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
	 * Returns the operations performed on simple values (or Strings in Redis terminology) 
	 * bound to the given key.
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
	 * Returns the operations performed on zset values (also known as sorted sets)
	 * bound to the given key.
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


	List<V> sort(SortQuery<K> query);

	<T> List<T> sort(SortQuery<K> query, RedisSerializer<T> resultSerializer);

	<T> List<T> sort(SortQuery<K> query, BulkMapper<T, V> bulkMapper);

	<T, S> List<T> sort(SortQuery<K> query, BulkMapper<T, S> bulkMapper, RedisSerializer<S> resultSerializer);

	Long sort(SortQuery<K> query, K storeKey);

	RedisSerializer<?> getValueSerializer();

	RedisSerializer<?> getKeySerializer();
}