/*
 * Copyright 2010-2011  original author or authors.
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
 * 
 * @author Costin Leau
 */
public class DefaultSortParameters implements SortParameters {

	private byte[] byPattern;
	private Range limit;
	private byte[] getPattern;
	private Order order;
	private Boolean alphabetic;

	/**
	 * Constructs a new <code>DefaultSortParameters</code> instance.
	 */
	public DefaultSortParameters() {
		this(null, null, null, null, null);
	}

	/**
	 * Constructs a new <code>DefaultSortParameters</code> instance.
	 *
	 * @param limit
	 * @param order
	 * @param alphabetic
	 */
	public DefaultSortParameters(Range limit, Order order, Boolean alphabetic) {
		this(null, limit, null, order, alphabetic);
	}

	/**
	 * Constructs a new <code>DefaultSortParameters</code> instance.
	 *
	 * @param byPattern
	 * @param limit
	 * @param getPattern
	 * @param order
	 * @param alphabetic
	 */
	public DefaultSortParameters(byte[] byPattern, Range limit, byte[] getPattern, Order order, Boolean alphabetic) {
		super();
		this.byPattern = byPattern;
		this.limit = limit;
		this.getPattern = getPattern;
		this.order = order;
		this.alphabetic = alphabetic;
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

	//
	// builder like methods
	//

	public SortParameters order(Order order) {
		setOrder(order);
		return this;
	}

	public SortParameters alpha() {
		setAlphabetic(true);
		return this;
	}

	public SortParameters numeric() {
		setAlphabetic(false);
		return this;
	}

	public SortParameters get(byte[] pattern) {
		setGetPattern(pattern);
		return this;
	}

	public SortParameters by(byte[] pattern) {
		setByPattern(pattern);
		return this;
	}

	public SortParameters limit(long start, long count) {
		setLimit(new Range(start, count));
		return this;
	}
}