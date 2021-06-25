/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.lang.Nullable;

/**
 * Redis map specific operations working on a hash.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 */
public interface HashOperations<H, HK, HV> {

	/**
	 * Delete given hash {@code hashKeys}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long delete(H key, Object... hashKeys);

	/**
	 * Determine if given hash {@code hashKey} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean hasKey(H key, Object hashKey);

	/**
	 * Get value for given {@code hashKey} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when key or hashKey does not exist or used in pipeline / transaction.
	 */
	@Nullable
	HV get(H key, Object hashKey);

	/**
	 * Get values for given {@code hashKeys} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> multiGet(H key, Collection<HK> hashKeys);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long increment(H key, HK hashKey, long delta);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Double increment(H key, HK hashKey, double delta);

	/**
	 * Return a random field from the hash value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	HK randomField(H key);

	/**
	 * Return a random field from the hash value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map.Entry<HK, HV> randomValue(H key);

	/**
	 * Return a random field from the hash value stored at {@code key}. If the provided {@code count} argument is
	 * positive, return a list of distinct fields, capped either at {@code count} or the hash size. If {@code count} is
	 * negative, the behavior changes and the command is allowed to return the same field multiple times. In this case,
	 * the number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	List<HK> randomFields(H key, long count);

	/**
	 * Return a random field from the hash value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return. Must be positive.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	@Nullable
	Map<HK, HV> randomValues(H key, long count);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Set<HK> keys(H key);

	/**
	 * Returns the length of the value associated with {@code hashKey}. If either the {@code key} or the {@code hashKey}
	 * do not exist, {@code 0} is returned.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 2.1
	 */
	@Nullable
	Long lengthOfValue(H key, HK hashKey);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Long size(H key);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m}.
	 *
	 * @param key must not be {@literal null}.
	 * @param m must not be {@literal null}.
	 */
	void putAll(H key, Map<? extends HK, ? extends HV> m);

	/**
	 * Set the {@code value} of a hash {@code hashKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 */
	void put(H key, HK hashKey, HV value);

	/**
	 * Set the {@code value} of a hash {@code hashKey} only if {@code hashKey} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Boolean putIfAbsent(H key, HK hashKey, HV value);

	/**
	 * Get entry set (values) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	List<HV> values(H key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return {@literal null} when used in pipeline / transaction.
	 */
	Map<HK, HV> entries(H key);

	/**
	 * Use a {@link Cursor} to iterate over entries in hash at {@code key}. <br />
	 * <strong>Important:</strong> Call {@link Cursor#close()} when done to avoid resource leak.
	 *
	 * @param key must not be {@literal null}.
	 * @param options
	 * @return {@literal null} when used in pipeline / transaction.
	 * @since 1.4
	 */
	Cursor<Map.Entry<HK, HV>> scan(H key, ScanOptions options);

	/**
	 * @return never {@literal null}.
	 */
	RedisOperations<H, ?> getOperations();
}
