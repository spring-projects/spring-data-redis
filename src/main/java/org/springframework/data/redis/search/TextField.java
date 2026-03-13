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
 * A full-text {@link SchemaField} supporting stemming, phonetics, and weighted scoring.
 * <p>
 * Create instances via {@link SchemaField#text(String)}.
 *
 * @author Viktoriya Kutsarova
 * @see SchemaField#text(String)
 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE — TEXT field options</a>
 */
public final class TextField extends SchemaField {

	private boolean sortable;
	private boolean unnormalized;
	private boolean noStem;
	private @Nullable PhoneticMatcher phonetic;
	private double weight = 1.0;
	private boolean withSuffixTrie;
	private boolean indexEmpty;

	TextField(String name) {
		super(name);
	}

	/**
	 * Enable low-latency sorting on this field ({@code SORTABLE}).
	 */
	public TextField sortable() {
		this.sortable = true;
		return this;
	}

	/**
	 * Enable sortable without normalization ({@code SORTABLE UNF}).
	 * Preserves the original case and diacritics for sorting.
	 */
	public TextField sortableUnnormalized() {
		this.sortable = true;
		this.unnormalized = true;
		return this;
	}

	/**
	 * Disable stemming for this field ({@code NOSTEM}).
	 */
	public TextField noStem() {
		this.noStem = true;
		return this;
	}

	/**
	 * Enable phonetic matching using the given matcher ({@code PHONETIC}).
	 *
	 * @param matcher the phonetic matching algorithm to use
	 * @see PhoneticMatcher
	 */
	public TextField phonetic(PhoneticMatcher matcher) {
		this.phonetic = matcher;
		return this;
	}

	/**
	 * Set the relevance weight for this field ({@code WEIGHT}). Default is {@code 1.0}.
	 *
	 * @param weight relevance multiplier; higher values increase ranking priority.
	 */
	public TextField weight(double weight) {
		this.weight = weight;
		return this;
	}

	/**
	 * Optimize suffix queries (e.g. {@code *word}) using a suffix trie ({@code WITHSUFFIXTRIE}).
	 */
	public TextField withSuffixTrie() {
		this.withSuffixTrie = true;
		return this;
	}

	/**
	 * Index documents where this field is present but contains an empty string ({@code INDEXEMPTY}).
	 */
	public TextField indexEmpty() {
		this.indexEmpty = true;
		return this;
	}

	public boolean isNoStem() {
		return noStem;
	}

	public @Nullable PhoneticMatcher getPhonetic() {
		return phonetic;
	}

	public double getWeight() {
		return weight;
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
