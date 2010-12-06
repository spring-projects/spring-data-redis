/*
 * Copyright 2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required byPattern applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.keyvalue.redis.connection;


/**
 * Default implementation for {@link SortParameters}.
 * @author Costin Leau
 */
public class DefaultSortParameters implements SortParameters {

	private byte[] byPattern;
	private Range limit;
	private byte[] getPattern;
	private byte[] hashKey;
	private Order order;
	private Boolean alphabetic;
	private byte[] storeKey;

	/**
	 * Constructs a new <code>DefaultSortParameters</code> instance.
	 */
	public DefaultSortParameters() {
		this(null, null, null, null, null, null, null);
	}

	/**
	 * Constructs a new <code>DefaultSortParameters</code> instance.
	 *
	 * @param limit
	 * @param order
	 * @param alphabetic
	 */
	public DefaultSortParameters(Range limit, Order order, Boolean alphabetic) {
		this(null, limit, null, null, order, alphabetic, null);
	}

	/**
	 * Constructs a new <code>DefaultSortParameters</code> instance.
	 *
	 * @param byPattern
	 * @param limit
	 * @param getPattern
	 * @param order
	 * @param alphabetic
	 * @param storeKey
	 */
	public DefaultSortParameters(byte[] by, Range limit, byte[] get, byte[] hashKey, Order order, Boolean alphabetic,
			byte[] storeKey) {
		super();
		this.byPattern = by;
		this.limit = limit;
		this.getPattern = get;
		this.hashKey = hashKey;
		this.order = order;
		this.alphabetic = alphabetic;
		this.storeKey = storeKey;
	}

	@Override
	public byte[] getByPattern() {
		return byPattern;
	}

	public void setByPattern(byte[] byPattern) {
		this.byPattern = byPattern;
	}

	@Override
	public Range getLimit() {
		return limit;
	}

	public void setLimit(Range limit) {
		this.limit = limit;
	}

	@Override
	public byte[] getGetPattern() {
		return getPattern;
	}

	public void setGetPattern(byte[] getPattern) {
		this.getPattern = getPattern;
	}

	@Override
	public byte[] getHashKey() {
		return hashKey;
	}

	public void setHashKey(byte[] hashKey) {
		this.hashKey = hashKey;
	}

	@Override
	public Order getOrder() {
		return order;
	}

	public void setOrder(Order order) {
		this.order = order;
	}

	@Override
	public Boolean isAlphabetic() {
		return alphabetic;
	}

	public void setAlphabetic(Boolean alphabetic) {
		this.alphabetic = alphabetic;
	}

	@Override
	public byte[] getStoreKey() {
		return storeKey;
	}

	public void setStoreKey(byte[] storeKey) {
		this.storeKey = storeKey;
	}
}