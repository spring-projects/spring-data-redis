/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.List;
import java.util.Set;

/**
 * Key-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisKeyCommands {

	/**
	 * Determine if given {@code key} exists.
	 * 
	 * @see http://redis.io/commands/exists
	 * @param key
	 * @return
	 */
	Boolean exists(byte[] key);

	/**
	 * Delete given {@code keys}.
	 * 
	 * @see http://redis.io/commands/del
	 * @param keys
	 * @return The number of keys that were removed.
	 */
	Long del(byte[]... keys);

	/**
	 * Determine the type stored at {@code key}.
	 * 
	 * @see http://redis.io/commands/type
	 * @param key
	 * @return
	 */
	DataType type(byte[] key);

	/**
	 * Find all keys matching the given {@code pattern}.
	 * 
	 * @see http://redis.io/commands/keys
	 * @param pattern
	 * @return
	 */
	Set<byte[]> keys(byte[] pattern);

	/**
	 * Return a random key from the keyspace.
	 * 
	 * @see http://redis.io/commands/randomkey
	 * @return
	 */
	byte[] randomKey();

	/**
	 * Rename key {@code oleName} to {@code newName}.
	 * 
	 * @see http://redis.io/commands/rename
	 * @param oldName
	 * @param newName
	 */
	void rename(byte[] oldName, byte[] newName);

	/**
	 * Rename key {@code oleName} to {@code newName} only if {@code newName} does not exist.
	 * 
	 * @see http://redis.io/commands/renamenx
	 * @param oldName
	 * @param newName
	 * @return
	 */
	Boolean renameNX(byte[] oldName, byte[] newName);

	/**
	 * Set time to live for given {@code key} in seconds.
	 * 
	 * @see http://redis.io/commands/expire
	 * @param key
	 * @param seconds
	 * @return
	 */
	Boolean expire(byte[] key, long seconds);

	/**
	 * Set time to live for given {@code key} in milliseconds.
	 * 
	 * @see http://redis.io/commands/pexpire
	 * @param key
	 * @param millis
	 * @return
	 */
	Boolean pExpire(byte[] key, long millis);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp.
	 * 
	 * @see http://redis.io/commands/expireat
	 * @param key
	 * @param unixTime
	 * @return
	 */
	Boolean expireAt(byte[] key, long unixTime);

	/**
	 * Set the expiration for given {@code key} as a {@literal UNIX} timestamp in milliseconds.
	 * 
	 * @see http://redis.io/commands/pexpireat
	 * @param key
	 * @param unixTimeInMillis
	 * @return
	 */
	Boolean pExpireAt(byte[] key, long unixTimeInMillis);

	/**
	 * Remove the expiration from given {@code key}.
	 * 
	 * @see http://redis.io/commands/persist
	 * @param key
	 * @return
	 */
	Boolean persist(byte[] key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 * 
	 * @see http://redis.io/commands/move
	 * @param key
	 * @param dbIndex
	 * @return
	 */
	Boolean move(byte[] key, int dbIndex);

	/**
	 * Get the time to live for {@code key} in seconds.
	 * 
	 * @see http://redis.io/commands/ttl
	 * @param key
	 * @return
	 */
	Long ttl(byte[] key);

	/**
	 * Get the time to live for {@code key} in milliseconds.
	 * 
	 * @see http://redis.io/commands/pttl
	 * @param key
	 * @return
	 */
	Long pTtl(byte[] key);

	/**
	 * Sort the elements for {@code key}.
	 * 
	 * @see http://redis.io/commands/sort
	 * @param key
	 * @param params
	 * @return
	 */
	List<byte[]> sort(byte[] key, SortParameters params);

	/**
	 * Sort the elements for {@code key} and store result in {@code storeKey}.
	 * 
	 * @see http://redis.io/commands/sort
	 * @param key
	 * @param params
	 * @param storeKey
	 * @return
	 */
	Long sort(byte[] key, SortParameters params, byte[] storeKey);

	/**
	 * Retrieve serialized version of the value stored at {@code key}.
	 * 
	 * @see http://redis.io/commands/dump
	 * @param key
	 * @return
	 */
	byte[] dump(byte[] key);

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(byte[])}.
	 * 
	 * @see http://redis.io/commands/restore
	 * @param key
	 * @param ttlInMillis
	 * @param serializedValue
	 */
	void restore(byte[] key, long ttlInMillis, byte[] serializedValue);
}
