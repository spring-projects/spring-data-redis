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
package org.springframework.data.redis.core.query;

import java.util.List;

import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.SortParameters.Range;

/**
 * Default SortQuery implementation.
 * 
 * @author Costin Leau
 */
class DefaultSortQuery<K> implements SortQuery<K> {

	private final K key;
	private final Boolean alpha;
	private final Order order;
	private final Range limit;
	private final String by;
	private final List<String> gets;

	DefaultSortQuery(K key, String by, Range limit, Order order, Boolean alpha, List<String> gets) {
		this.key = key;
		this.by = by;
		this.limit = limit;
		this.order = order;
		this.alpha = alpha;
		this.gets = gets;
	}

	@Override
	public String getBy() {
		return by;
	}

	@Override
	public Range getLimit() {
		return limit;
	}

	@Override
	public Order getOrder() {
		return order;
	}

	@Override
	public Boolean isAlphabetic() {
		return alpha;
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public List<String> getGetPattern() {
		return gets;
	}

	@Override
	public String toString() {
		return "DefaultSortQuery [alpha=" + alpha + ", by=" + by + ", gets=" + gets + ", key=" + key + ", limit="
				+ limit + ", order=" + order + "]";
	}


}