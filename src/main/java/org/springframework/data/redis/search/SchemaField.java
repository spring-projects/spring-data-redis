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

import org.jspecify.annotations.Nullable;

/**
 * Abstract base for a field in a Redis Search index schema. Provides factory methods for creating
 * type-specific field definitions and common configuration options shared across all field types.
 * <p>
 * Use the static factory methods to create fields:
 * <pre class="code">
 * Schema schema = Schema.of(
 *     SchemaField.text("title").weight(2.0).sortable(),
 *     SchemaField.tag("status").separator(';'),
 *     SchemaField.numeric("price").sortable(),
 *     SchemaField.geo("location")
 * );
 * </pre>
 * <p>
 * With static imports for more concise code:
 * <pre class="code">
 * import static org.springframework.data.redis.search.SchemaField.*;
 *
 * Schema schema = Schema.of(
 *     text("title").weight(2.0).sortable(),
 *     tag("status"),
 *     numeric("price").sortable()
 * );
 * </pre>
 *
 * @author Viktoriya Kutsarova
 * @see TextField
 * @see TagField
 * @see NumericField
 * @see GeoField
 */
public abstract class SchemaField {

	private final String name;
	private @Nullable String alias;
	private boolean indexed = true;
	private boolean indexMissing = false;

	protected SchemaField(String name) {
		this.name = name;
	}

	/**
	 * Create a full-text {@link TextField} for the given field name.
	 *
	 * @param name field name
	 * @return a new text field
	 */
	public static TextField text(String name) {
		return new TextField(name);
	}

	/**
	 * Create a {@link TagField} for the given field name.
	 *
	 * @param name field name
	 * @return a new tag field
	 */
	public static TagField tag(String name) {
		return new TagField(name);
	}

	/**
	 * Create a {@link NumericField} for the given field name.
	 *
	 * @param name field name
	 * @return a new numeric field
	 */
	public static NumericField numeric(String name) {
		return new NumericField(name);
	}

	/**
	 * Create a {@link GeoField} for the given field name.
	 *
	 * @param name field name
	 * @return a new geo field
	 */
	public static GeoField geo(String name) {
		return new GeoField(name);
	}

	/**
	 * Set an alternative attribute name used in query expressions ({@code AS alias}).
	 *
	 * @param alias the alias name
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public <T extends SchemaField> T as(String alias) {
		this.alias = alias;
		return (T) this;
	}

	/**
	 * Configure whether this field should be indexed ({@code NOINDEX} when disabled).
	 * <p>
	 * When set to {@code false}, the field is only stored for retrieval but not searchable.
	 * <p>
	 * Default is {@code true} (field is indexed).
	 *
	 * @param indexed {@code true} to index this field, {@code false} to store only
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public <T extends SchemaField> T indexed(boolean indexed) {
		this.indexed = indexed;
		return (T) this;
	}

	/**
	 * Index documents where this field is absent ({@code INDEXMISSING}).
	 * Enables searching for documents that do not have this attribute set.
	 *
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public <T extends SchemaField> T indexMissing() {
		this.indexMissing = true;
		return (T) this;
	}

	/**
	 * Return the field name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return the field alias, or {@code null} if not set.
	 */
	public @Nullable String getAlias() {
		return alias;
	}

	/**
	 * Return whether this field is indexed.
	 */
	public boolean isIndexed() {
		return indexed;
	}

	/**
	 * Return whether documents missing this field should be indexed.
	 */
	public boolean isIndexMissing() {
		return indexMissing;
	}
}

