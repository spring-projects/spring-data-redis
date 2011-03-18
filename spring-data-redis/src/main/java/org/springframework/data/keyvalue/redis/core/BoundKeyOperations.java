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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;

/**
 * Operations over a Redis key.  
 * 
 * Useful for executing common key-'bound' operations to all implementations.
 * 
 * <p>As the rest of the APIs, if the underlying connection is pipelined or queued/in multi mode,
 * all methods will return null. In such scenarios, to prevent any data inconsistencies, mutative
 * methods that query the store (such as {@link #renameIfAbsent(Object)} or {@link #move(int)}) will throw
 * an exception.
 * 
 * </p>
 * @author Costin Leau
 */
public interface BoundKeyOperations<K> {

	/**
	 * Returns the key associated with this entity.
	 * 
	 * @return key associated with the implementing entity
	 */
	K getKey();

	/**
	 * Returns the associated Redis type.
	 *  
	 * @return key type
	 */
	DataType getType();

	/**
	 * Returns the expiration of this key. 
	 * 
	 * @return expiration value (in seconds)
	 */
	Long getExpire();

	/**
	 * Sets the key time-to-live/expiration.
	 * 
	 * @param timeout expiration value
	 * @param unit expiration unit
	 * @return true if expiration was set, false otherwise
	 */
	Boolean expire(long timeout, TimeUnit unit);

	/**
	 * Sets the key time-to-live/expiration.
	 * 
	 * @param date expiration date
	 * @return true if expiration was set, false otherwise
	 */
	Boolean expireAt(Date date);

	/**
	 * Removes the expiration (if any) of the key.
	 * @return true if expiration was removed, false otherwise
	 */
	Boolean persist();

	/**
	 * Renames the key.
	 * 
	 * @param newKey new key
	 */
	void rename(K newKey);

	/**
	 * Renames the key (if the new key does not exist). Note that the underlying key
	 * changes only if the operation returns true (which does not happen if the connection
	 * is pipelined or in multi mode).
	 * 
	 * @param newKey new key
	 * @return true if rename was successful, false otherwise
	 */
	Boolean renameIfAbsent(K newKey);

	/**
	 * Moves the key (if it exists) to the specified database. If the key already exists in the 
	 * destination database, or it does not exist in the source database, it does nothing.
	 * <p/>
	 * As opposed to the raw move command, the database of the underlying connection is switched as well
	 * to the new database.
	 * 
	 * @see RedisConnection#select(int)
	 * @see RedisConnection#move(byte[], int)
	 * @param dbIndex database index
	 * @return true if the operation succeed, false otherwise
	 */
	Boolean move(int dbIndex);
}