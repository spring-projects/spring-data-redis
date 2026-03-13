/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.search;

/**
 * A numeric {@link SchemaField} supporting range queries and sorting.
 * <p>
 * Create instances via {@link SchemaField#numeric(String)}.
 *
 * @author Viktoriya Kutsarova
 * @see SchemaField#numeric(String)
 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE — NUMERIC field options</a>
 */
public final class NumericField extends SchemaField {

	private boolean sortable;
	private boolean unnormalized;

	NumericField(String name) {
		super(name);
	}

	/**
	 * Enable low-latency sorting on this field ({@code SORTABLE}).
	 */
	public NumericField sortable() {
		this.sortable = true;
		return this;
	}

	/**
	 * Enable sortable without normalization ({@code SORTABLE UNF}).
	 * Preserves the original case and diacritics for sorting.
	 */
	public NumericField sortableUnnormalized() {
		this.sortable = true;
		this.unnormalized = true;
		return this;
	}

	public boolean isSortable() {
		return sortable;
	}

	public boolean isUnnormalized() {
		return unnormalized;
	}
}
