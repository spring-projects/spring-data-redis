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
package org.springframework.data.redis.core.query;

import java.util.List;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.SortParameters.Range;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;

/**
 * High-level abstraction over a Redis SORT (generified equivalent of {@link SortParameters}). To be used with
 * {@link RedisTemplate} (just as {@link SortParameters} is used by {@link RedisConnection}).
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public interface SortQuery<K> {

	/**
	 * Return the target key for sorting.
	 *
	 * @return the target key
	 */
	K getKey();

	/**
	 * Returns the sorting order. Can be null if nothing is specified.
	 *
	 * @return sorting order
	 */
	@Nullable
	Order getOrder();

	/**
	 * Indicates if the sorting is numeric (default) or alphabetical (lexicographical). Can be null if nothing is
	 * specified.
	 *
	 * @return the type of sorting
	 */
	@Nullable
	Boolean isAlphabetic();

	/**
	 * Returns the sorting limit (range or pagination). Can be null if nothing is specified.
	 *
	 * @return sorting limit/range
	 */
	@Nullable
	Range getLimit();

	/**
	 * Returns the pattern of the external key used for sorting.
	 *
	 * @return the external key pattern
	 */
	@Nullable
	String getBy();

	/**
	 * Returns the external key(s) whose values are returned by the sort.
	 *
	 * @return the (list of) keys used for GET
	 */
	List<String> getGetPattern();
}
