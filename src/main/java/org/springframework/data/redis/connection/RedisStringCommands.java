/*
 * Copyright 2011-2017 the original author or authors.
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
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	byte[] get(byte[] key);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	byte[] getSet(byte[] key, byte[] value);

	/**
	 * Get multiple {@code keys}. Values are returned in the order of the requested keys.
	 *
	 * @param keys  must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/mget">Redis Documentation: MGET</a>
	 */
	List<byte[]> mGet(byte[]... keys);

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	void set(byte[] key, byte[] value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration can be {@literal null}. Defaulted to {@link Expiration#persistent()}.
	 * @param option can be {@literal null}. Defaulted to {@link SetOption#UPSERT}.
	 * @since 1.7
	 * @see <a href="http://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	void set(byte[] key, byte[] value, Expiration expiration, SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/setnx">Redis Documentation: SETNX</a>
	 */
	Boolean setNX(byte[] key, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	void setEx(byte[] key, long seconds, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @since 1.3
	 * @see <a href="http://redis.io/commands/psetex">Redis Documentation: PSETEX</a>
	 */
	void pSetEx(byte[] key, long milliseconds, byte[] value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param tuple  must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/mset">Redis Documentation: MSET</a>
	 */
	void mSet(Map<byte[], byte[]> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param tuple must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/msetnx">Redis Documentation: MSETNX</a>
	 */
	Boolean mSetNX(Map<byte[], byte[]> tuple);

	/**
	 * Increment an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/incr">Redis Documentation: INCR</a>
	 */
	Long incr(byte[] key);

	/**
	 * Increment an integer value stored of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/incrby">Redis Documentation: INCRBY</a>
	 */
	Long incrBy(byte[] key, long value);

	/**
	 * Increment a floating point number value of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 */
	Double incrBy(byte[] key, double value);

	/**
	 * Decrement an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/decr">Redis Documentation: DECR</a>
	 */
	Long decr(byte[] key);

	/**
	 * Decrement an integer value stored as string value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/decrby">Redis Documentation: DECRBY</a>
	 */
	Long decrBy(byte[] key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 * @see <a href="http://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	Long append(byte[] key, byte[] value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	byte[] getRange(byte[] key, long begin, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="http://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	void setRange(byte[] key, byte[] value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return
	 * @see <a href="http://redis.io/commands/getbit">Redis Documentation: GETBIT</a>
	 */
	Boolean getBit(byte[] key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset}.
	 * @see <a href="http://redis.io/commands/setbit">Redis Documentation: SETBIT</a>
	 */
	Boolean setBit(byte[] key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/bitcount">Redis Documentation: BITCOUNT</a>
	 */
	Long bitCount(byte[] key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/bitcount">Redis Documentation: BITCOUNT</a>
	 */
	Long bitCount(byte[] key, long begin, long end);

	/**
	 * Perform bitwise operations between strings.
	 *
	 * @param op  must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/bitop">Redis Documentation: BITOP</a>
	 */
	Long bitOp(BitOperation op, byte[] destination, byte[]... keys);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
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
