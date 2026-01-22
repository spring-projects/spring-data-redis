/*
 * Copyright 2011-present the original author or authors.
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

/**
 * String/Value-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @author Viktoriya Kutsarova
 * @see RedisCommands
 */
@NullUnmarked
public interface RedisStringCommands {

	enum BitOperation {

		AND, OR, XOR, NOT,

		/**
		 * @since 4.1
		 */
		DIFF,

		/**
		 * @since 4.1
		 */
		DIFF1,

		/**
		 * @since 4.1
		 */
		ANDOR,

		/**
		 * @since 4.1
		 */
		ONE;
	}

	/**
	 * Get the value of {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/get">Redis Documentation: GET</a>
	 */
	byte[] get(byte @NonNull [] key);

	/**
	 * Return the value at {@code key} and delete the key.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getdel">Redis Documentation: GETDEL</a>
	 * @since 2.6
	 */
	byte[] getDel(byte @NonNull [] key);

	/**
	 * Return the value at {@code key} and expire the key by applying {@link Expiration}.
	 * <p>
	 * Use {@link Expiration#seconds(long)} for {@code EX}. <br />
	 * Use {@link Expiration#milliseconds(long)} for {@code PX}. <br />
	 * Use {@link Expiration#unixTimestamp(long, TimeUnit)} for {@code EXAT | PXAT}. <br />
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getex">Redis Documentation: GETEX</a>
	 * @since 2.6
	 */
	byte[] getEx(byte @NonNull [] key, @NonNull Expiration expiration);

	/**
	 * Set {@code value} of {@code key} and return its old value.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} if key did not exist before or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getset">Redis Documentation: GETSET</a>
	 */
	byte[] getSet(byte @NonNull [] key, byte @NonNull [] value);

	/**
	 * Get multiple {@code keys}. Values are in the order of the requested keys Absent field values are represented using
	 * {@literal null} in the resulting {@link List}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/mget">Redis Documentation: MGET</a>
	 */
	List<byte[]> mGet(byte @NonNull [] @NonNull... keys);

	/**
	 * Set {@code value} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean set(byte @NonNull [] key, byte @NonNull [] value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration must not be {@literal null}. Use {@link Expiration#persistent()} to not set any ttl or
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @param option must not be {@literal null}. Use {@link SetOption#upsert()} to add non existing.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.7
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean set(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration, @NonNull SetOption option);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} depending on {@code condition}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration must not be {@literal null}. Use {@link Expiration#persistent()} to not set any ttl or
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @param condition must not be {@literal null}. Use {@link SetCondition#upsert()} to add non-existing.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 4.1.0
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	Boolean set(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration, @Nullable SetCondition condition);

	/**
	 * Set {@code value} for {@code key}. Return the old string stored at key, or {@literal null} if key did not exist. An
	 * error is returned and SET aborted if the value stored at key is not a string.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration must not be {@literal null}. Use {@link Expiration#persistent()} to not set any ttl or
	 *          {@link Expiration#keepTtl()} to keep the existing expiration.
	 * @param option must not be {@literal null}. Use {@link SetOption#upsert()} to add non-existing.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 3.5
	 * @see <a href="https://redis.io/commands/set">Redis Documentation: SET</a>
	 */
	byte[] setGet(byte @NonNull [] key, byte @NonNull [] value, @NonNull Expiration expiration,
			@NonNull SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setnx">Redis Documentation: SETNX</a>
	 */
	Boolean setNX(byte @NonNull [] key, byte @NonNull [] value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setex">Redis Documentation: SETEX</a>
	 */
	Boolean setEx(byte @NonNull [] key, long seconds, byte @NonNull [] value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.3
	 * @see <a href="https://redis.io/commands/psetex">Redis Documentation: PSETEX</a>
	 */
	Boolean pSetEx(byte @NonNull [] key, long milliseconds, byte @NonNull [] value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 *
	 * @param tuple must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/mset">Redis Documentation: MSET</a>
	 */
	Boolean mSet(@NonNull Map<byte @NonNull [], byte @NonNull []> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 *
	 * @param tuple must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/msetnx">Redis Documentation: MSETNX</a>
	 */
	Boolean mSetNX(@NonNull Map<byte @NonNull [], byte @NonNull []> tuple);

	/**
	 * Increment an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incr">Redis Documentation: INCR</a>
	 */
	Long incr(byte @NonNull [] key);

	/**
	 * Increment an integer value stored of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrby">Redis Documentation: INCRBY</a>
	 */
	Long incrBy(byte @NonNull [] key, long value);

	/**
	 * Increment a floating point number value of {@code key} by {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/incrbyfloat">Redis Documentation: INCRBYFLOAT</a>
	 */
	Double incrBy(byte @NonNull [] key, double value);

	/**
	 * Decrement an integer value stored as string value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/decr">Redis Documentation: DECR</a>
	 */
	Long decr(byte @NonNull [] key);

	/**
	 * Decrement an integer value stored as string value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/decrby">Redis Documentation: DECRBY</a>
	 */
	Long decrBy(byte @NonNull [] key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/append">Redis Documentation: APPEND</a>
	 */
	Long append(byte @NonNull [] key, byte[] value);

	/**
	 * Get a substring of value of {@code key} between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getrange">Redis Documentation: GETRANGE</a>
	 */
	byte[] getRange(byte @NonNull [] key, long start, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 * @see <a href="https://redis.io/commands/setrange">Redis Documentation: SETRANGE</a>
	 */
	void setRange(byte @NonNull [] key, byte @NonNull [] value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/getbit">Redis Documentation: GETBIT</a>
	 */
	Boolean getBit(byte @NonNull [] key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset} or {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/setbit">Redis Documentation: SETBIT</a>
	 */
	Boolean setBit(byte @NonNull [] key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/bitcount">Redis Documentation: BITCOUNT</a>
	 */
	Long bitCount(byte @NonNull [] key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code start} and
	 * {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/bitcount">Redis Documentation: BITCOUNT</a>
	 */
	Long bitCount(byte @NonNull [] key, long start, long end);

	/**
	 * Get / Manipulate specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset stored
	 * at a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param subCommands must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	List<Long> bitField(byte @NonNull [] key, @NonNull BitFieldSubCommands subCommands);

	/**
	 * Perform bitwise operations between strings.
	 *
	 * @param op must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/bitop">Redis Documentation: BITOP</a>
	 */
	Long bitOp(@NonNull BitOperation op, byte @NonNull [] destination, byte @NonNull [] @NonNull... keys);

	/**
	 * Return the position of the first bit set to given {@code bit} in a string.
	 *
	 * @param key the key holding the actual String.
	 * @param bit the bit value to look for.
	 * @return {@literal null} when used in pipeline / transaction. The position of the first bit set to 1 or 0 according
	 *         to the request.
	 * @see <a href="https://redis.io/commands/bitpos">Redis Documentation: BITPOS</a>
	 * @since 2.1
	 */
	default Long bitPos(byte @NonNull [] key, boolean bit) {
		return bitPos(key, bit, Range.unbounded());
	}

	/**
	 * Return the position of the first bit set to given {@code bit} in a string. {@link Range} start and end can contain
	 * negative values in order to index <strong>bytes</strong> starting from the end of the string, where {@literal -1}
	 * is the last byte, {@literal -2} is the penultimate.
	 *
	 * @param key the key holding the actual String.
	 * @param bit the bit value to look for.
	 * @param range must not be {@literal null}. Use {@link Range#unbounded()} to not limit search.
	 * @return {@literal null} when used in pipeline / transaction. The position of the first bit set to 1 or 0 according
	 *         to the request.
	 * @see <a href="https://redis.io/commands/bitpos">Redis Documentation: BITPOS</a>
	 * @since 2.1
	 */
	Long bitPos(byte @NonNull [] key, boolean bit, @NonNull Range<@NonNull Long> range);

	/**
	 * Get the length of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/strlen">Redis Documentation: STRLEN</a>
	 */
	Long strLen(byte @NonNull [] key);

	/**
	 * {@code SET} command arguments for {@code NX}, {@code XX}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	enum SetOption {

		/**
		 * Do not set any additional command argument.
		 */
		UPSERT,

		/**
		 * {@code NX}
		 */
		SET_IF_ABSENT,

		/**
		 * {@code XX}
		 */
		SET_IF_PRESENT;

		/**
		 * Do not set any additional command argument.
		 */
		public static SetOption upsert() {
			return UPSERT;
		}

		/**
		 * {@code XX}
		 */
		public static SetOption ifPresent() {
			return SET_IF_PRESENT;
		}

		/**
		 * {@code NX}
		 */
		public static SetOption ifAbsent() {
			return SET_IF_ABSENT;
		}
	}

	/**
	 * {@code SET} command condition arguments for {@code NX}, {@code XX}, {@code IFEQ}.
	 * <p>
	 * Supports compare-and-swap (CAS) and compare-and-delete (CAD) semantics introduced in Redis 8.4.
	 *
	 * @author Yordan Tsintsov
	 * @see <a href="https://redis.io/commands/set">Redis SET command</a>
	 * @since 4.1.0
	 */
	class SetCondition {

		// Cached instances for stateless conditions
		private static final SetCondition UPSERT_INSTANCE = new SetCondition(Type.UPSERT, null);
		private static final SetCondition IF_ABSENT_INSTANCE = new SetCondition(Type.SET_IF_ABSENT, null);
		private static final SetCondition IF_PRESENT_INSTANCE = new SetCondition(Type.SET_IF_PRESENT, null);

		private final @NonNull Type type;
		private final byte @Nullable [] compareValue;

		private SetCondition(@NonNull Type type, byte @Nullable [] compareValue) {
			this.type = type;
			this.compareValue = compareValue == null ? null : Arrays.copyOf(compareValue, compareValue.length);
		}

		/**
		 * Creates a condition that always sets the value, regardless of whether the key exists.
		 * <p>
		 * This is the default Redis {@code SET} behavior when no condition is specified.
		 *
		 * @return a cached {@link SetCondition} instance representing no precondition.
		 */
		public static SetCondition upsert() {
			return UPSERT_INSTANCE;
		}

		/**
		 * Creates a condition that sets the value only if the key does not already exist.
		 * <p>
		 * Corresponds to the Redis {@code NX} option.
		 *
		 * @return a cached {@link SetCondition} instance for the {@code NX} condition.
		 */
		public static SetCondition ifAbsent() {
			return IF_ABSENT_INSTANCE;
		}

		/**
		 * Creates a condition that sets the value only if the key already exists.
		 * <p>
		 * Corresponds to the Redis {@code XX} option.
		 *
		 * @return a cached {@link SetCondition} instance for the {@code XX} condition.
		 */
		public static SetCondition ifPresent() {
			return IF_PRESENT_INSTANCE;
		}

		/**
		 * Creates a condition that sets the value only if the current value stored at the key
		 * equals the specified {@code value}.
		 * <p>
		 * The operation will succeed only if:
		 * <ul>
		 *   <li>The key exists, AND</li>
		 *   <li>The current value exactly matches the provided {@code value}</li>
		 * </ul>
		 * <p>
		 * Corresponds to the Redis {@code IFEQ} option introduced in Redis 8.4.
		 * <p>
		 * <b>Note:</b> Empty byte arrays are valid comparison values.
		 *
		 * @param value the expected current value to compare against; must not be {@literal null}.
		 * @return a new {@link SetCondition} instance for the {@code IFEQ} condition.
		 * @throws IllegalArgumentException if {@code value} is {@literal null}.
		 */
		public static SetCondition ifValueEqual(byte @NonNull [] value) {
			Assert.notNull(value, "Value must not be null");
			return new SetCondition(Type.SET_IF_VALUE_EQUAL, value);
		}

		/**
		 * Returns the type of condition represented by this instance.
		 *
		 * @return the condition {@link Type}; never {@literal null}.
		 */
		public @NonNull Type getType() {
			return this.type;
		}

		/**
		 * Returns the comparison value or {@literal null}.
		 * <p>
		 * <b>Note:</b> A defensive copy of the internal byte array is returned to preserve immutability.
		 *
		 * @return a copy of the comparison value, or {@literal null}
		 */
		public byte @Nullable [] getCompareValue() {
			if (this.compareValue == null) {
				return null;
			}
			return Arrays.copyOf(this.compareValue, this.compareValue.length);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SetCondition that = (SetCondition) o;
			return type == that.type && Objects.deepEquals(compareValue, that.compareValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(type, Arrays.hashCode(compareValue));
		}

		@Override
		public String toString() {
			return "%s{type=%s, compareValue=%s}".formatted(getClass().getSimpleName(), type, compareValue != null ? "*****" : "<none>");
		}

		/**
		 * {@code SET} command options for {@code NX}, {@code XX}, {@code IFEQ}.
		 *
		 * @author Yordan Tsintsov
		 * @since 4.1.0
		 */
		public enum Type {

			/**
			 * Do not set any additional command argument.
			 */
			UPSERT,

			/**
			 * {@code NX}
			 */
			SET_IF_ABSENT,

			/**
			 * {@code NX}
			 */
			SET_IF_PRESENT,

			/**
			 * {@code IFEQ}
			 */
			SET_IF_VALUE_EQUAL,

		}

	}

}
