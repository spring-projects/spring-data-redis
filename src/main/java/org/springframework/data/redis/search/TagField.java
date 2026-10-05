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
public final class TagField extends TextualSchemaField<TagField> {

	private char separator = ',';
	private boolean caseSensitive = false;

	TagField(String name) {
		super(name);
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

	public char getSeparator() {
		return separator;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}
}
