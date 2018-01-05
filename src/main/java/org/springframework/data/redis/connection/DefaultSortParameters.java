/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.lang.Nullable;

/**
 * Default implementation for {@link SortParameters}.
 *
 * @author Costin Leau
 */
public class DefaultSortParameters implements SortParameters {

	private @Nullable byte[] byPattern;
	private @Nullable Range limit;
	private final List<byte[]> getPattern = new ArrayList<>(4);
	private @Nullable Order order;
	private @Nullable Boolean alphabetic;

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
	public DefaultSortParameters(@Nullable Range limit, @Nullable Order order, @Nullable Boolean alphabetic) {
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
	public DefaultSortParameters(@Nullable byte[] byPattern, @Nullable Range limit, @Nullable byte[][] getPattern,
			@Nullable Order order, @Nullable Boolean alphabetic) {
		super();
		this.byPattern = byPattern;
		this.limit = limit;
		this.order = order;
		this.alphabetic = alphabetic;
		setGetPattern(getPattern);
	}

	@Nullable
	public byte[] getByPattern() {
		return byPattern;
	}

	public void setByPattern(byte[] byPattern) {
		this.byPattern = byPattern;
	}

	public Range getLimit() {
		return limit;
	}

	public void setLimit(Range limit) {
		this.limit = limit;
	}

	public byte[][] getGetPattern() {
		return getPattern.toArray(new byte[getPattern.size()][]);
	}

	@Nullable
	public void addGetPattern(byte[] gPattern) {
		getPattern.add(gPattern);
	}

	public void setGetPattern(@Nullable byte[][] gPattern) {
		getPattern.clear();

		if (gPattern == null) {
			return;
		}

		Collections.addAll(getPattern, gPattern);
	}

	@Nullable
	public Order getOrder() {
		return order;
	}

	public void setOrder(Order order) {
		this.order = order;
	}

	@Nullable
	public Boolean isAlphabetic() {
		return alphabetic;
	}

	public void setAlphabetic(Boolean alphabetic) {
		this.alphabetic = alphabetic;
	}

	//
	// builder like methods
	//

	public DefaultSortParameters order(Order order) {
		setOrder(order);
		return this;
	}

	public DefaultSortParameters alpha() {
		setAlphabetic(true);
		return this;
	}

	public DefaultSortParameters asc() {
		setOrder(Order.ASC);
		return this;
	}

	public DefaultSortParameters desc() {
		setOrder(Order.DESC);
		return this;
	}

	public DefaultSortParameters numeric() {
		setAlphabetic(false);
		return this;
	}

	public DefaultSortParameters get(byte[] pattern) {
		addGetPattern(pattern);
		return this;
	}

	public DefaultSortParameters by(byte[] pattern) {
		setByPattern(pattern);
		return this;
	}

	public DefaultSortParameters limit(long start, long count) {
		setLimit(new Range(start, count));
		return this;
	}
}
