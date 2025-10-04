/*
 * Copyright 2011-2025 the original author or authors.
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
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Set-specific commands supported by Redis.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mingi Lee
 * @see RedisCommands
 */
@NullUnmarked
public interface RedisSetCommands {

	/**
	 * Add given {@code values} to set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be empty.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sadd">Redis Documentation: SADD</a>
	 */
	Long sAdd(byte @NonNull [] key, byte @NonNull [] @NonNull... values);

	/**
	 * Remove given {@code values} from set at {@code key} and return the number of removed elements.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be empty.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srem">Redis Documentation: SREM</a>
	 */
	Long sRem(byte @NonNull [] key, byte @NonNull []... values);

	/**
	 * Remove and return a random member from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when key does not exist or used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 */
	byte[] sPop(byte @NonNull [] key);

	/**
	 * Remove and return {@code count} random members from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of random members to pop from the set.
	 * @return empty {@link List} if set does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/spop">Redis Documentation: SPOP</a>
	 * @since 2.0
	 */
	List<byte @NonNull []> sPop(byte @NonNull [] key, long count);

	/**
	 * Move {@code value} from {@code srcKey} to {@code destKey}
	 *
	 * @param srcKey must not be {@literal null}.
	 * @param destKey must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/smove">Redis Documentation: SMOVE</a>
	 */
	Boolean sMove(byte @NonNull [] srcKey, byte @NonNull [] destKey, byte @NonNull [] value);

	/**
	 * Get size of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/scard">Redis Documentation: SCARD</a>
	 */
	Long sCard(byte @NonNull [] key);

	/**
	 * Check if set at {@code key} contains {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sismember">Redis Documentation: SISMEMBER</a>
	 */
	Boolean sIsMember(byte @NonNull [] key, byte @NonNull [] value);

	/**
	 * Check if set at {@code key} contains one or more {@code values}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/smismember">Redis Documentation: SMISMEMBER</a>
	 */
	List<@NonNull Boolean> sMIsMember(byte @NonNull [] key, byte @NonNull [] @NonNull... values);

	/**
	 * Diff all sets for given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiff">Redis Documentation: SDIFF</a>
	 */
	Set<byte @NonNull []> sDiff(byte @NonNull [] @NonNull... keys);

	/**
	 * Diff all sets for given {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sdiffstore">Redis Documentation: SDIFFSTORE</a>
	 */
	Long sDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys);

	/**
	 * Returns the members intersecting all given sets at {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return empty {@link Set} if no intersection found. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinter">Redis Documentation: SINTER</a>
	 */
	Set<byte @NonNull []> sInter(byte @NonNull [] @NonNull... keys);

	/**
	 * Intersect all given sets at {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sinterstore">Redis Documentation: SINTERSTORE</a>
	 */
	Long sInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys);

	/**
	 * Returns the cardinality of the set which would result from the intersection of all the given sets.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sintercard">Redis Documentation: SINTERCARD</a>
	 * @since 4.0
	 */
	Long sInterCard(byte @NonNull [] @NonNull... keys);

	/**
	 * Union all sets at given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return empty {@link Set} if keys do not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunion">Redis Documentation: SUNION</a>
	 */
	Set<byte @NonNull []> sUnion(byte @NonNull [] @NonNull... keys);

	/**
	 * Union all sets at given {@code keys} and store result in {@code destKey}.
	 *
	 * @param destKey must not be {@literal null}.
	 * @param keys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/sunionstore">Redis Documentation: SUNIONSTORE</a>
	 */
	Long sUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull... keys);

	/**
	 * Get all elements of set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return empty {@link Set} when key does not exist. {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/smembers">Redis Documentation: SMEMBERS</a>
	 */
	Set<byte @NonNull []> sMembers(byte @NonNull [] key);

	/**
	 * Get random element from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	byte[] sRandMember(byte @NonNull [] key);

	/**
	 * Get {@code count} random elements from set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count
	 * @return {@literal null} when used in pipeline / transaction.
	 * @see <a href="https://redis.io/commands/srandmember">Redis Documentation: SRANDMEMBER</a>
	 */
	List<byte @NonNull []> sRandMember(byte @NonNull [] key, long count);

	/**
	 * Use a {@link Cursor} to iterate over elements in set at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.4
	 * @see <a href="https://redis.io/commands/scan">Redis Documentation: SCAN</a>
	 */
	Cursor<byte @NonNull []> sScan(byte @NonNull [] key, ScanOptions options);
}
