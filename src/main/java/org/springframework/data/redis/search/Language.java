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
 * Languages supported by Redis Search for text field stemming.
 * <p>
 * The language setting affects how words are stemmed during indexing and search.
 * For example, in English, "running" and "runs" would both stem to "run".
 *
 * @author Viktoriya Kutsarova
 * @see IndexDefinition#language(Language)
 * @see <a href="https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/stemming/">Stemming</a>
 */
public enum Language {

	ARABIC("arabic"),
	ARMENIAN("armenian"),
	CHINESE("chinese"),
	DANISH("danish"),
	DUTCH("dutch"),
	ENGLISH("english"),
	FINNISH("finnish"),
	FRENCH("french"),
	GERMAN("german"),
	HUNGARIAN("hungarian"),
	ITALIAN("italian"),
	NORWEGIAN("norwegian"),
	PORTUGUESE("portuguese"),
	ROMANIAN("romanian"),
	RUSSIAN("russian"),
	SERBIAN("serbian"),
	SPANISH("spanish"),
	SWEDISH("swedish"),
	TAMIL("tamil"),
	TURKISH("turkish"),
	YIDDISH("yiddish");

	private final String value;

	Language(String value) {
		this.value = value;
	}

	/**
	 * Return the Redis protocol value for this language (lowercase language name).
	 */
	public String getValue() {
		return value;
	}
}

