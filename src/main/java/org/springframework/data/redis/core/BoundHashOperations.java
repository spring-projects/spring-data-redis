/*
 * Copyright 2011-2016 the original author or authors.
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

/**
 * Hash operations bound to a certain key.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Ninad Divadkar
 */
public interface BoundHashOperations<H, HK, HV> extends BoundKeyOperations<H> {

	/**
	 * @return
	 */
	RedisOperations<H, ?> getOperations();

	/**
	 * Determine if given hash {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Boolean hasKey(Object key);

	/**
	 * Increment {@code value} of a hash {@code key} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return
	 */
	Long increment(HK key, long delta);

	/**
	 * Increment {@code value} of a hash {@code key} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param delta
	 * @return
	 */
	Double increment(HK key, double delta);

	/**
	 * Get value for given {@code key} from the hash.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	HV get(Object key);

	/**
	 * Set the {@code value} of a hash {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 */
	void put(HK key, HV value);

	/**
	 * Set the {@code value} of a hash {@code key} only if {@code key} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Boolean putIfAbsent(HK key, HV value);

	/**
	 * Get values for given {@code keys} from the hash.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	List<HV> multiGet(Collection<HK> keys);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m}.
	 *
	 * @param m must not be {@literal null}.
	 */
	void putAll(Map<? extends HK, ? extends HV> m);

	/**
	 * Get key set (fields) of the hash.
	 *
	 * @return
	 */
	Set<HK> keys();

	/**
	 * Get entry set (values) of hash.
	 *
	 * @return
	 */
	List<HV> values();

	/**
	 * Get size of the hash.
	 *
	 * @return
	 */
	Long size();

	/**
	 * Delete given hash {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Long delete(Object... keys);

	/**
	 * Get entire hash.
	 *
	 * @return
	 */
	Map<HK, HV> entries();

	/**
	 * Use a {@link Cursor} to iterate over entries in the hash.
	 *
	 * @param options
	 * @return
	 * @since 1.4
	 */
	Cursor<Map.Entry<HK, HV>> scan(ScanOptions options);
}
