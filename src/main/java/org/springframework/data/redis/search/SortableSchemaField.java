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
 * Abstract base for {@link SchemaField schema fields} that support sorting.
 * <p>
 * Sortable fields can be used with {@code SORTBY} in search queries for low-latency sorting.
 * The {@link #normalized(boolean)} option controls whether values are normalized for sorting
 * (lowercased, diacritics removed) or preserved in their original form.
 *
 * @param <T> the concrete field type for fluent method chaining
 * @author Viktoriya Kutsarova
 * @see SchemaField
 * @see TextField
 * @see TagField
 * @see NumericField
 * @see GeoField
 */
public abstract class SortableSchemaField<T extends SortableSchemaField<T>> extends SchemaField {

	private boolean sortable = false;
	private boolean normalized = true;

	protected SortableSchemaField(String name) {
		super(name);
	}

	/**
	 * Enable low-latency sorting on this field ({@code SORTABLE}).
	 *
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public T sortable() {
		this.sortable = true;
		return (T) this;
	}

	/**
	 * Configure whether sortable values are normalized ({@code UNF} when disabled).
	 * <p>
	 * When set to {@code false}, preserves the original case and diacritics for sorting.
	 * This option only applies when {@link #sortable()} is enabled.
	 * <p>
	 * Default is {@code true} (values are normalized).
	 *
	 * @param normalized {@code true} to normalize, {@code false} to preserve original form
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public T normalized(boolean normalized) {
		this.normalized = normalized;
		return (T) this;
	}

	/**
	 * Return whether this field is sortable.
	 */
	public boolean isSortable() {
		return sortable;
	}

	/**
	 * Return whether sortable values are normalized.
	 */
	public boolean isNormalized() {
		return normalized;
	}
}

