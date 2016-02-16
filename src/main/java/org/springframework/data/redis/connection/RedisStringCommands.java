/*
 * Copyright 2011-2016 the original author or authors.
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

import org.springframework.data.redis.core.types.Expiration;

/**
 * String/Value-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisStringCommands {

	public enum BitOperation {
		AND, OR, XOR, NOT;
	}

	/**
	 * Get the value of {@code key}.
	 * <p>
	 * See http://redis.io/commands/get
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	byte[] get(byte[] key);

	/**
	 * Set value of {@code key} and return its old value.
	 * <p>
	 * See http://redis.io/commands/getset
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	byte[] getSet(byte[] key, byte[] value);

	/**
	 * Get the values of all given {@code keys}.
	 * <p>
	 * See http://redis.io/commands/mget
	 * 
	 * @param keys
	 * @return
	 */
	List<byte[]> mGet(byte[]... keys);

	/**
	 * Set {@code value} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/set
	 * 
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 */
	void set(byte[] key, byte[] value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 * <p>
	 * See http://redis.io/commands/set
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration can be {@literal null}. Defaulted to {@link Expiration#persistent()}.
	 * @param option can be {@literal null}. Defaulted to {@link SetOption#UPSERT}.
	 * @since 1.7
	 */
	void set(byte[] key, byte[] value, Expiration expiration, SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 * <p>
	 * See http://redis.io/commands/setnx
	 * 
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	Boolean setNX(byte[] key, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/setex
	 * 
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 */
	void setEx(byte[] key, long seconds, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/psetex
	 * 
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @since 1.3
	 */
	void pSetEx(byte[] key, long milliseconds, byte[] value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 * <p>
	 * See http://redis.io/commands/mset
	 * 
	 * @param tuple
	 */
	void mSet(Map<byte[], byte[]> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 * <p>
	 * See http://redis.io/commands/msetnx
	 * 
	 * @param tuple
	 */
	Boolean mSetNX(Map<byte[], byte[]> tuple);

	/**
	 * Increment value of {@code key} by 1.
	 * <p>
	 * See http://redis.io/commands/incr
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long incr(byte[] key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/incrby
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long incrBy(byte[] key, long value);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/incrbyfloat
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Double incrBy(byte[] key, double value);

	/**
	 * Decrement value of {@code key} by 1.
	 * <p>
	 * See http://redis.io/commands/decr
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long decr(byte[] key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/decrby
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long decrBy(byte[] key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 * <p>
	 * See http://redis.io/commands/append
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long append(byte[] key, byte[] value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 * <p>
	 * See http://redis.io/commands/getrange
	 * 
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	byte[] getRange(byte[] key, long begin, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 * <p>
	 * See http://redis.io/commands/setrange
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 */
	void setRange(byte[] key, byte[] value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 * <p>
	 * See http://redis.io/commands/getbit
	 * 
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return
	 */
	Boolean getBit(byte[] key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/setbit
	 * 
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset}.
	 */
	Boolean setBit(byte[] key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/bitcount
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long bitCount(byte[] key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 * <p>
	 * See http://redis.io/commands/bitcount
	 * 
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	Long bitCount(byte[] key, long begin, long end);

	/**
	 * Perform bitwise operations between strings.
	 * <p>
	 * See http://redis.io/commands/bitop
	 * 
	 * @param op
	 * @param destination
	 * @param keys
	 * @return
	 */
	Long bitOp(BitOperation op, byte[] destination, byte[]... keys);

	/**
	 * Get the length of the value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/strlen
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long strLen(byte[] key);

	/**
	 * {@code SET} command arguments for {@code NX}, {@code XX}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static enum SetOption {

		/**
		 * Do not set any additional command argument.
		 * 
		 * @return
		 */
		UPSERT,

		/**
		 * {@code NX}
		 * 
		 * @return
		 */
		SET_IF_ABSENT,

		/**
		 * {@code XX}
		 * 
		 * @return
		 */
		SET_IF_PRESENT;

		/**
		 * Do not set any additional command argument.
		 * 
		 * @return
		 */
		public static SetOption upsert() {
			return UPSERT;
		}

		/**
		 * {@code XX}
		 * 
		 * @return
		 */
		public static SetOption ifPresent() {
			return SET_IF_PRESENT;
		}

		/**
		 * {@code NX}
		 * 
		 * @return
		 */
		public static SetOption ifAbsent() {
			return SET_IF_ABSENT;
		}
	}
}
