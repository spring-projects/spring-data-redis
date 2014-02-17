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
import java.util.Map;

/**
 * String/Value-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisStringCommands {

	public enum BitOperation {
		AND, OR, XOR, NOT;
	}

	/**
	 * Get the value of {@code key}.
	 * 
	 * @see http://redis.io/commands/get
	 * @param key
	 * @return
	 */
	byte[] get(byte[] key);

	/**
	 * Set value of {@code key} and return its old value.
	 * 
	 * @see http://redis.io/commands/getset
	 * @param key
	 * @param value
	 * @return
	 */
	byte[] getSet(byte[] key, byte[] value);

	/**
	 * Get the values of all given {@code keys}.
	 * 
	 * @see http://redis.io/commands/mget
	 * @param keys
	 * @return
	 */
	List<byte[]> mGet(byte[]... keys);

	/**
	 * Set {@code value} for {@code key}.
	 * 
	 * @see http://redis.io/commands/set
	 * @param key
	 * @param value
	 */
	void set(byte[] key, byte[] value);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 * 
	 * @see http://redis.io/commands/setnx
	 * @param key
	 * @param value
	 * @return
	 */
	Boolean setNX(byte[] key, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 * 
	 * @see http://redis.io/commands/setex
	 * @param key
	 * @param seconds
	 * @param value
	 */
	void setEx(byte[] key, long seconds, byte[] value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 * 
	 * @see http://redis.io/commands/mset
	 * @param tuple
	 */
	void mSet(Map<byte[], byte[]> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 * 
	 * @see http://redis.io/commands/msetnx
	 * @param tuple
	 */
	Boolean mSetNX(Map<byte[], byte[]> tuple);

	/**
	 * Increment value of {@code key} by 1.
	 * 
	 * @see http://redis.io/commands/incr
	 * @param key
	 * @return
	 */
	Long incr(byte[] key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * 
	 * @see http://redis.io/commands/incrby
	 * @param key
	 * @return
	 */
	Long incrBy(byte[] key, long value);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * 
	 * @see http://redis.io/commands/incrbyfloat
	 * @param key
	 * @return
	 */
	Double incrBy(byte[] key, double value);

	/**
	 * Decrement value of {@code key} by 1.
	 * 
	 * @see http://redis.io/commands/decr
	 * @param key
	 * @return
	 */
	Long decr(byte[] key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * 
	 * @see http://redis.io/commands/decrby
	 * @param key
	 * @param value
	 * @return
	 */
	Long decrBy(byte[] key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 * 
	 * @see http://redis.io/commands/append
	 * @param key
	 * @param value
	 * @return
	 */
	Long append(byte[] key, byte[] value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 * 
	 * @see http://redis.io/commands/getrange
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	byte[] getRange(byte[] key, long begin, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 * 
	 * @see http://redis.io/commands/setrange
	 * @param key
	 * @param value
	 * @param offset
	 */
	void setRange(byte[] key, byte[] value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 * 
	 * @see http://redis.io/commands/getbit
	 * @param key
	 * @param offset
	 * @return
	 */
	Boolean getBit(byte[] key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 * 
	 * @see http://redis.io/commands/setbit
	 * @param key
	 * @param offset
	 * @param value
	 */
	void setBit(byte[] key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 * 
	 * @see http://redis.io/commands/bitcount
	 * @param key
	 * @return
	 */
	Long bitCount(byte[] key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 * 
	 * @see http://redis.io/commands/bitcount
	 * @param key
	 * @param begin
	 * @param end
	 * @return
	 */
	Long bitCount(byte[] key, long begin, long end);

	/**
	 * Perform bitwise operations between strings.
	 * 
	 * @see http://redis.io/commands/bitop
	 * @param op
	 * @param destination
	 * @param keys
	 * @return
	 */
	Long bitOp(BitOperation op, byte[] destination, byte[]... keys);

	/**
	 * Get the length of the value stored at {@code key}.
	 * 
	 * @see http://redis.io/commands/strlen
	 * @param key
	 * @return
	 */
	Long strLen(byte[] key);
}
