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

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Set-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisSetCommands {

	/**
	 * Add given {@code values} to set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/sadd
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	Long sAdd(byte[] key, byte[]... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 * <p>
	 * See http://redis.io/commands/srem
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	Long sRem(byte[] key, byte[]... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/spop
	 * 
	 * @param key
	 * @return
	 */
	byte[] sPop(byte[] key);

	/**
	 * Move {@code value} from {@code srcKey} to {@code destKey}
	 * <p>
	 * See http://redis.io/commands/smove
	 * 
	 * @param srcKey
	 * @param destKey
	 * @param value
	 * @return
	 */
	Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value);

	/**
	 * Get size of set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/scard
	 * 
	 * @param key
	 * @return
	 */
	Long sCard(byte[] key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 * <p>
	 * See http://redis.io/commands/sismember
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	Boolean sIsMember(byte[] key, byte[] value);

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 * <p>
	 * See http://redis.io/commands/sinter
	 * 
	 * @param keys
	 * @return
	 */
	Set<byte[]> sInter(byte[]... keys);

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destKey}.
	 * <p>
	 * See http://redis.io/commands/sinterstore
	 * 
	 * @param destKey
	 * @param keys
	 * @return
	 */
	Long sInterStore(byte[] destKey, byte[]... keys);

	/**
	 * Union all sets at given {@code keys}.
	 * <p>
	 * See http://redis.io/commands/sunion
	 * 
	 * @param keys
	 * @return
	 */
	Set<byte[]> sUnion(byte[]... keys);

	/**
	 * Union all sets at given {@code keys} and store result in {@code destKey}.
	 * <p>
	 * See http://redis.io/commands/sunionstore
	 * 
	 * @param destKey
	 * @param keys
	 * @return
	 */
	Long sUnionStore(byte[] destKey, byte[]... keys);

	/**
	 * Diff all sets for given {@code keys}.
	 * <p>
	 * See http://redis.io/commands/sdiff
	 * 
	 * @param keys
	 * @return
	 */
	Set<byte[]> sDiff(byte[]... keys);

	/**
	 * Diff all sets for given {@code keys} and store result in {@code destKey}
	 * <p>
	 * See http://redis.io/commands/sdiffstore
	 * 
	 * @param destKey
	 * @param keys
	 * @return
	 */
	Long sDiffStore(byte[] destKey, byte[]... keys);

	/**
	 * Get all elements of set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/smembers
	 * 
	 * @param key
	 * @return
	 */
	Set<byte[]> sMembers(byte[] key);

	/**
	 * Get random element from set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/srandmember
	 * 
	 * @param key
	 * @return
	 */
	byte[] sRandMember(byte[] key);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/srandmember
	 * 
	 * @param key
	 * @param count
	 * @return
	 */
	List<byte[]> sRandMember(byte[] key, long count);

	/**
	 * Use a {@link Cursor} to iterate over elements in set at {@code key}.
	 * <p>
	 * See http://redis.io/commands/scan
	 * 
	 * @since 1.4
	 * @param key
	 * @param options
	 * @return
	 */
	Cursor<byte[]> sScan(byte[] key, ScanOptions options);
}
