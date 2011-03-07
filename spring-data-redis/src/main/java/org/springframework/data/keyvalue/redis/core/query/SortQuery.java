/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core.query;

import org.springframework.data.keyvalue.redis.connection.SortParameters.Order;
import org.springframework.data.keyvalue.redis.connection.SortParameters.Range;

/**
 * @author Costin Leau
 */
public interface SortQuery<K> {

	/**
	 * Returns the sorting order. Can be null if nothing is specified.
	 * 
	 * @return sorting order
	 */
	Order getOrder();

	/**
	 * Indicates if the sorting is numeric (default) or alphabetical (lexicographical).
	 * Can be null if nothing is specified.
	 * 
	 * @return the type of sorting
	 */
	Boolean isAlphabetic();


	/**
	 * Returns the sorting limit (range or pagination).
	 * Can be null if nothing is specified.
	 * 
	 * @return sorting limit/range
	 */
	Range getLimit();

	/**
	 * Target key for sorting.
	 * 
	 * @return
	 */
	K getKey();

	/**
	 * Pattern of external key used for sorting.
	 * 
	 * @return 
	 */
	String getBy();
}