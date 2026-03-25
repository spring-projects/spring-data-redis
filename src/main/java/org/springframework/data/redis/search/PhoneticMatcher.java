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
 * Represents a phonetic matching algorithm for Redis Search fuzzy text matching.
 * <p>
 * Phonetic matching allows finding words that sound similar, useful for handling
 * misspellings and variations in user queries.
 * <p>
 * Well-known matchers are provided as constants (e.g., {@link #ENGLISH}, {@link #FRENCH}).
 * Custom or newly supported matchers can be created via {@link #of(String)}.
 *
 * @author Viktoriya Kutsarova
 * @see TextField#phonetic(PhoneticMatcher)
 * @see <a href="https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/phonetic_matching/">Phonetic Matching</a>
 */
public final class PhoneticMatcher {

	/**
	 * Double Metaphone algorithm for English.
	 */
	public static final PhoneticMatcher ENGLISH = new PhoneticMatcher("dm:en");

	/**
	 * Double Metaphone algorithm for French.
	 */
	public static final PhoneticMatcher FRENCH = new PhoneticMatcher("dm:fr");

	/**
	 * Double Metaphone algorithm for Portuguese.
	 */
	public static final PhoneticMatcher PORTUGUESE = new PhoneticMatcher("dm:pt");

	/**
	 * Double Metaphone algorithm for Spanish.
	 */
	public static final PhoneticMatcher SPANISH = new PhoneticMatcher("dm:es");

	private final String value;

	private PhoneticMatcher(String value) {
		this.value = value;
	}

	/**
	 * Create a {@link PhoneticMatcher} from a Redis phonetic matcher value.
	 *
	 * @param value the Redis phonetic matcher value (e.g., {@code "dm:en"})
	 */
	public static PhoneticMatcher of(String value) {
		return new PhoneticMatcher(value);
	}

	/**
	 * Return the Redis protocol value for this matcher (e.g., {@code "dm:en"}).
	 */
	public String getValue() {
		return value;
	}
}

