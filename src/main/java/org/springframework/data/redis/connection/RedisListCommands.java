/*
 * Copyright 2011-2014 the original author or authors.
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

/**
 * List-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisListCommands {

	/**
	 * List insertion position.
	 */
	public enum Position {
		BEFORE, AFTER
	}

	/**
	 * Append {@code values} to {@code key}.
	 * <p>
	 * See http://redis.io/commands/rpush
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	Long rPush(byte[] key, byte[]... values);

	/**
	 * Prepend {@code values} to {@code key}.
	 * <p>
	 * See http://redis.io/commands/lpush
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	Long lPush(byte[] key, byte[]... values);

	/**
	 * Append {@code} values to {@code key} only if the list exists.
	 * <p>
	 * See http://redis.io/commands/rpushx
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	Long rPushX(byte[] key, byte[] value);

	/**
	 * Prepend {@code values} to {@code key} only if the list exists.
	 * <p>
	 * See http://redis.io/commands/lpushx
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	Long lPushX(byte[] key, byte[] value);

	/**
	 * Get the size of list stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/llen
	 * 
	 * @param key
	 * @return
	 */
	Long lLen(byte[] key);

	/**
	 * Get elements between {@code begin} and {@code end} from list at {@code key}.
	 * <p>
	 * See http://redis.io/commands/lrange
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	List<byte[]> lRange(byte[] key, long begin, long end);

	/**
	 * Trim list at {@code key} to elements between {@code begin} and {@code end}.
	 * <p>
	 * See http://redis.io/commands/ltrim
	 * 
	 * @param key
	 * @param begin
	 * @param end
	 */
	void lTrim(byte[] key, long begin, long end);

	/**
	 * Get element at {@code index} form list at {@code key}.
	 * <p>
	 * See http://redis.io/commands/lindex
	 * 
	 * @param key
	 * @param index
	 * @return
	 */
	byte[] lIndex(byte[] key, long index);

	/**
	 * Insert {@code value} {@link Position#BEFORE} or {@link Position#AFTER} existing {@code pivot} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/linsert
	 * 
	 * @param key
	 * @param where
	 * @param pivot
	 * @param value
	 * @return
	 */
	Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value);

	/**
	 * Set the {@code value} list element at {@code index}.
	 * <p>
	 * See http://redis.io/commands/lset
	 * 
	 * @param key
	 * @param index
	 * @param value
	 */
	void lSet(byte[] key, long index, byte[] value);

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/lrem
	 * 
	 * @param key
	 * @param count
	 * @param value
	 * @return
	 */
	Long lRem(byte[] key, long count, byte[] value);

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/lpop
	 * 
	 * @param key
	 * @return
	 */
	byte[] lPop(byte[] key);

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/rpop
	 * 
	 * @param key
	 * @return
	 */
	byte[] rPop(byte[] key);

	/**
	 * Removes and returns first element from lists stored at {@code keys} (see: {@link #lPop(byte[])}). <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 * <p>
	 * See http://redis.io/commands/blpop
	 * 
	 * @param timeout
	 * @param keys
	 * @return
	 */
	List<byte[]> bLPop(int timeout, byte[]... keys);

	/**
	 * Removes and returns last element from lists stored at {@code keys} (see: {@link #rPop(byte[])}). <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 * <p>
	 * See http://redis.io/commands/brpop
	 * 
	 * @param timeout
	 * @param keys
	 * @return
	 */
	List<byte[]> bRPop(int timeout, byte[]... keys);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value.
	 * <p>
	 * See http://redis.io/commands/rpoplpush
	 * 
	 * @param srcKey
	 * @param dstKey
	 * @return
	 */
	byte[] rPopLPush(byte[] srcKey, byte[] dstKey);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value (see
	 * {@link #rPopLPush(byte[], byte[])}). <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 * <p>
	 * See http://redis.io/commands/brpoplpush
	 * 
	 * @param timeout
	 * @param srcKey
	 * @param dstKey
	 * @return
	 */
	byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey);
}
