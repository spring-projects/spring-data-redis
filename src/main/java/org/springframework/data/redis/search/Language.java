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

import java.util.Locale;

/**
 * Represents a language for Redis Search text field stemming.
 * <p>
 * The language setting affects how words are stemmed during indexing and search.
 * For example, in English, "running" and "runs" would both stem to "run".
 * <p>
 * Well-known languages are provided as constants (e.g., {@link #ENGLISH}, {@link #FRENCH}).
 * Custom or newly supported languages can be created via {@link #of(String)} or {@link #of(Locale)}.
 *
 * @author Viktoriya Kutsarova
 * @see IndexDefinition#language(Language)
 * @see <a href="https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/stemming/">Stemming</a>
 */
public final class Language {

	public static final Language ARABIC = new Language("arabic");
	public static final Language ARMENIAN = new Language("armenian");
	public static final Language CHINESE = new Language("chinese");
	public static final Language DANISH = new Language("danish");
	public static final Language DUTCH = new Language("dutch");
	public static final Language ENGLISH = new Language("english");
	public static final Language FINNISH = new Language("finnish");
	public static final Language FRENCH = new Language("french");
	public static final Language GERMAN = new Language("german");
	public static final Language HUNGARIAN = new Language("hungarian");
	public static final Language ITALIAN = new Language("italian");
	public static final Language NORWEGIAN = new Language("norwegian");
	public static final Language PORTUGUESE = new Language("portuguese");
	public static final Language ROMANIAN = new Language("romanian");
	public static final Language RUSSIAN = new Language("russian");
	public static final Language SERBIAN = new Language("serbian");
	public static final Language SPANISH = new Language("spanish");
	public static final Language SWEDISH = new Language("swedish");
	public static final Language TAMIL = new Language("tamil");
	public static final Language TURKISH = new Language("turkish");
	public static final Language YIDDISH = new Language("yiddish");

	private final String value;

	private Language(String value) {
		this.value = value;
	}

	/**
	 * Create a {@link Language} from a Redis language name.
	 *
	 * @param value the Redis language name (e.g., {@code "catalan"})
	 */
	public static Language of(String value) {
		return new Language(value);
	}

	/**
	 * Create a {@link Language} from a {@link Locale}.
	 * <p>
	 * The locale's English display language name is used as the Redis language value
	 * (e.g., {@code Locale.FRENCH} becomes {@code "french"}).
	 *
	 * @param locale the locale to derive the language from
	 */
	public static Language of(Locale locale) {
		return new Language(locale.getDisplayLanguage(Locale.ENGLISH).toLowerCase(Locale.ENGLISH));
	}

	/**
	 * Return the Redis protocol value for this language (lowercase language name).
	 */
	public String getValue() {
		return value;
	}
}

