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

import java.util.Map;

/**
 * The field values of a single document returned by a Redis Search {@code FT.SEARCH} query.
 * <p>
 * Keys are the field names or aliases as defined by {@code AS} or {@code RETURN} in the query.
 * <p>
 * An empty document is returned when {@code NOCONTENT} is set on the search.
 *
 * @author Viktoriya Kutsarova
 * @see SearchHit
 * @see SearchResult
 */
public class SearchDocument {

	private final Map<String, String> fields;

	/**
	 * Create a new {@link SearchDocument} wrapping the given field map.
	 *
	 * @param fields field values from the server; never {@literal null}.
	 */
	public SearchDocument(Map<String, String> fields) {
		this.fields = Map.copyOf(fields);
	}

	/**
	 * Return the field values.
	 */
	public Map<String, String> getFields() {
		return fields;
	}

	@Override
	public String toString() {
		return "SearchDocument{fields=" + fields + "}";
	}
}
