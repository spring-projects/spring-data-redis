/*
 * Copyright 2011-2023 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * List-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author dengliming
 */
public interface RedisListCommands {

	/**
	 * List insertion position.
	 */
	enum Position {
		BEFORE, AFTER
	}

	/**
	 * List move direction.
	 *
	 * @since 2.6
	 */
	enum Direction {

		LEFT, RIGHT;

		/**
		 * Alias for {@link Direction#LEFT}.
		 *
		 * @return
		 */
		public static Direction first() {
			return LEFT;
		}

		/**
		 * Alias for {@link Direction#RIGHT}.
		 *
		 * @return
		 */
		public static Direction last() {
			return RIGHT;
		}
	}

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be empty.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	@Nullable
	Long rPush(byte[] key, byte[]... values);

	/**
	 * Returns the index of matching elements inside the list stored at given {@literal key}. <br />
	 * Requires Redis 6.0.6 or newer.
	 *
	 * @param key must not be {@literal null}.
	 * @param element must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpos">Redis Documentation: LPOS</a>
	 * @since 2.4
	 */
	@Nullable
	default Long lPos(byte[] key, byte[] element) {
		return CollectionUtils.firstElement(lPos(key, element, null, null));
	}

	/**
	 * Returns the index of matching elements inside the list stored at given {@literal key}. <br />
	 * Requires Redis 6.0.6 or newer.
	 *
	 * @param key must not be {@literal null}.
	 * @param element must not be {@literal null}.
	 * @param rank specifies the "rank" of the first element to return, in case there are multiple matches. A rank of 1
	 *          means to return the first match, 2 to return the second match, and so forth.
	 * @param count number of matches to return.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpos">Redis Documentation: LPOS</a>
	 * @since 2.4
	 */
	@Nullable
	List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count);

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be empty.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	@Nullable
	Long lPush(byte[] key, byte[]... values);

	/**
	 * Append {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpushx">Redis Documentation: RPUSHX</a>
	 */
	@Nullable
	Long rPushX(byte[] key, byte[] value);

	/**
	 * Prepend {@code values} to {@code key} only if the list exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpushx">Redis Documentation: LPUSHX</a>
	 */
	@Nullable
	Long lPushX(byte[] key, byte[] value);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 */
	@Nullable
	Long lLen(byte[] key);

	/**
	 * Get elements between {@code start} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return empty {@link List} if key does not exists or range does not contain values. {@literal null} when used in
	 *         pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 */
	@Nullable
	List<byte[]> lRange(byte[] key, long start, long end);

	/**
	 * Trim list at {@code key} to elements between {@code start} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @see <a href="https://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 */
	void lTrim(byte[] key, long start, long end);

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index zero based index value. Use negative number to designate elements starting at the tail.
	 * @return {@literal null} when index is out of range or when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	@Nullable
	byte[] lIndex(byte[] key, long index);

	/**
	 * Insert {@code value} {@link Position#BEFORE} or {@link Position#AFTER} existing {@code pivot} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param where must not be {@literal null}.
	 * @param pivot must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 */
	@Nullable
	Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value);

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/lmove">Redis Documentation: LMOVE</a>
	 * @see #bLMove(byte[], byte[], Direction, Direction, double)
	 */
	@Nullable
	byte[] lMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to);

	/**
	 * Atomically returns and removes the first/last element (head/tail depending on the {@code from} argument) of the
	 * list stored at {@code sourceKey}, and pushes the element at the first/last element (head/tail depending on the
	 * {@code to} argument) of the list stored at {@code destinationKey}.
	 * <p>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param sourceKey must not be {@literal null}.
	 * @param destinationKey must not be {@literal null}.
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 * @param timeout
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/blmove">Redis Documentation: BLMOVE</a>
	 * @see #lMove(byte[], byte[], Direction, Direction)
	 */
	@Nullable
	byte[] bLMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to, double timeout);

	/**
	 * Set the {@code value} list element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value
	 * @see <a href="https://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	void lSet(byte[] key, long index, byte[] value);

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	@Nullable
	Long lRem(byte[] key, long count, byte[] value);

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 */
	@Nullable
	byte[] lPop(byte[] key);

	/**
	 * Removes and returns first {@code} elements in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 * @since 2.6
	 */
	@Nullable
	List<byte[]> lPop(byte[] key, long count);

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	@Nullable
	byte[] rPop(byte[] key);

	/**
	 * Removes and returns last {@code} elements in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 * @since 2.6
	 */
	@Nullable
	List<byte[]> rPop(byte[] key, long count);

	/**
	 * Removes and returns first element from lists stored at {@code keys}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout seconds to block.
	 * @param keys must not be {@literal null}.
	 * @return empty {@link List} when no element could be popped and the timeout was reached. {@literal null} when used
	 *         in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 * @see #lPop(byte[])
	 */
	@Nullable
	List<byte[]> bLPop(int timeout, byte[]... keys);

	/**
	 * Removes and returns last element from lists stored at {@code keys}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout seconds to block.
	 * @param keys must not be {@literal null}.
	 * @return empty {@link List} when no element could be popped and the timeout was reached. {@literal null} when used
	 *         in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 * @see #rPop(byte[])
	 */
	@Nullable
	List<byte[]> bRPop(int timeout, byte[]... keys);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value.
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	@Nullable
	byte[] rPopLPush(byte[] srcKey, byte[] dstKey);

	/**
	 * Remove the last element from list at {@code srcKey}, append it to {@code dstKey} and return its value. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param timeout seconds to block.
	 * @param srcKey must not be {@literal null}.
	 * @param dstKey must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 * @see #rPopLPush(byte[], byte[])
	 */
	@Nullable
	byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey);
}
