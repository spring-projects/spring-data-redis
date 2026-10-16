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
 * Abstract base for {@link SortableSchemaField sortable schema fields} that support suffix trie optimization
 * and empty value indexing. These options are shared by {@link TextField} and {@link TagField}.
 *
 * @param <T> the concrete field type for fluent method chaining
 * @author Viktoriya Kutsarova
 * @see TextField
 * @see TagField
 */
public abstract class TextualSchemaField<T extends TextualSchemaField<T>> extends SortableSchemaField<T> {

	private boolean suffixTrie = false;
	private boolean indexEmpty = false;

	protected TextualSchemaField(String name) {
		super(name);
	}

	/**
	 * Configure whether to use a suffix trie for optimizing suffix queries ({@code WITHSUFFIXTRIE}).
	 * <p>
	 * When enabled, queries like {@code *word} are optimized. This uses additional memory.
	 * <p>
	 * Default is {@code false} (suffix trie is not used).
	 *
	 * @param suffixTrie {@code true} to enable suffix trie, {@code false} to disable
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public T suffixTrie(boolean suffixTrie) {
		this.suffixTrie = suffixTrie;
		return (T) this;
	}

	/**
	 * Index documents where this field is present but contains an empty string ({@code INDEXEMPTY}).
	 * <p>
	 * Default is {@code false} (empty strings are not indexed).
	 *
	 * @return this field for method chaining
	 */
	@SuppressWarnings("unchecked")
	public T indexEmpty() {
		this.indexEmpty = true;
		return (T) this;
	}

	/**
	 * Return whether a suffix trie is used for this field.
	 */
	public boolean isSuffixTrie() {
		return suffixTrie;
	}

	/**
	 * Return whether empty strings are indexed for this field.
	 */
	public boolean isIndexEmpty() {
		return indexEmpty;
	}
}

