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

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.SortParameters.Range;

/**
 * Default implementation for {@link SortCriterion}.
 * 
 * @author Costin Leau
 */
class DefaultSortCriterion<K> implements SortCriterion<K> {

	private final K key;
	private String by;
	private final List<String> getKeys = new ArrayList<String>(4);

	private Range limit;
	private Order order;
	private Boolean alpha;

	DefaultSortCriterion(K key) {
		this.key = key;
	}

	
	public SortCriterion<K> alphabetical(boolean alpha) {
		this.alpha = Boolean.valueOf(alpha);
		return this;
	}

	
	public SortQuery<K> build() {
		return new DefaultSortQuery<K>(key, by, limit, order, alpha, getKeys);
	}

	
	public SortCriterion<K> limit(long offset, long count) {
		this.limit = new Range(offset, count);
		return this;
	}

	
	public SortCriterion<K> limit(Range range) {
		this.limit = range;
		return this;
	}

	
	public SortCriterion<K> order(Order order) {
		this.order = order;
		return this;
	}

	
	public SortCriterion<K> get(String getPattern) {
		this.getKeys.add(getPattern);
		return this;
	}

	SortCriterion<K> addBy(String keyPattern) {
		this.by = keyPattern;
		return this;
	}
}