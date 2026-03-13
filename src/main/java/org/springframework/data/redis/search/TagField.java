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
 * A tag {@link SchemaField} for exact-match queries on categorical values.
 * Tag fields are split on a configurable separator and matched verbatim (case-insensitive by default).
 * <p>
 * Create instances via {@link SchemaField#tag(String)}.
 *
 * @author Viktoriya Kutsarova
 * @see SchemaField#tag(String)
 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE — TAG field options</a>
 */
public final class TagField extends SchemaField {

	private boolean sortable;
	private boolean unnormalized;
	private char separator = ',';
	private boolean caseSensitive;
	private boolean withSuffixTrie;
	private boolean indexEmpty;

	TagField(String name) {
		super(name);
	}

	/**
	 * Enable low-latency sorting on this field ({@code SORTABLE}).
	 */
	public TagField sortable() {
		this.sortable = true;
		return this;
	}

	/**
	 * Enable sortable without normalization ({@code SORTABLE UNF}).
	 * Preserves the original case and diacritics for sorting.
	 */
	public TagField sortableUnnormalized() {
		this.sortable = true;
		this.unnormalized = true;
		return this;
	}

	/**
	 * Set the character used to split tag values ({@code SEPARATOR}). Default is {@code ','}.
	 * Must be a single character.
	 *
	 * @param separator split delimiter, e.g. {@code ';'}.
	 */
	public TagField separator(char separator) {
		this.separator = separator;
		return this;
	}

	/**
	 * Preserve the original case of tag values during indexing ({@code CASESENSITIVE}).
	 * By default tags are lowercased.
	 */
	public TagField caseSensitive() {
		this.caseSensitive = true;
		return this;
	}

	/**
	 * Optimize suffix queries using a suffix trie ({@code WITHSUFFIXTRIE}).
	 */
	public TagField withSuffixTrie() {
		this.withSuffixTrie = true;
		return this;
	}

	/**
	 * Index documents where this field is present but contains an empty string ({@code INDEXEMPTY}).
	 */
	public TagField indexEmpty() {
		this.indexEmpty = true;
		return this;
	}

	public char getSeparator() {
		return separator;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public boolean isWithSuffixTrie() {
		return withSuffixTrie;
	}

	public boolean isIndexEmpty() {
		return indexEmpty;
	}

	public boolean isSortable() {
		return sortable;
	}

	public boolean isUnnormalized() {
		return unnormalized;
	}
}
