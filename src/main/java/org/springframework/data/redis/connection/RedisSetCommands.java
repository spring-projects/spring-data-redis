/*
 * Copyright 2011-2018 the original author or authors.
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
import org.springframework.lang.Nullable;

/**
 * Set-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisSetCommands {

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be empty.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	@Nullable
	Long sAdd(byte[] key, byte[]... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be empty.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	@Nullable
	Long sRem(byte[] key, byte[]... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	@Nullable
	byte[] sPop(byte[] key);

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of random members to pop from the set.
	 * @return empty {@link List} if set does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 * @since 2.0
	 */
	@Nullable
	List<byte[]> sPop(byte[] key, long count);

	/**
	 * Move {@code value} from {@code srcKey} to {@code destKey}
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	@Nullable
	Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	@Nullable
	Long sCard(byte[] key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	@Nullable
	Boolean sIsMember(byte[] key, byte[] value);

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return empty {@link Set} if no intersection found. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	@Nullable
	Set<byte[]> sInter(byte[]... keys);

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	@Nullable
	Long sInterStore(byte[] destKey, byte[]... keys);

	/**
	 * Union all sets at given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return empty {@link Set} if keys do not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	@Nullable
	Set<byte[]> sUnion(byte[]... keys);

	/**
	 * Union all sets at given {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	@Nullable
	Long sUnionStore(byte[] destKey, byte[]... keys);

	/**
	 * Diff all sets for given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	@Nullable
	Set<byte[]> sDiff(byte[]... keys);

	/**
	 * Diff all sets for given {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	@Nullable
	Long sDiffStore(byte[] destKey, byte[]... keys);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link Set} when key does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	@Nullable
	Set<byte[]> sMembers(byte[] key);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	byte[] sRandMember(byte[] key);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="http://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	@Nullable
	List<byte[]> sRandMember(byte[] key, long count);

	/**
	 * Use a {@link Cursor} to iterate over elements in set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.4
	 * @see <a href="http://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	Cursor<byte[]> sScan(byte[] key, ScanOptions options);
}
