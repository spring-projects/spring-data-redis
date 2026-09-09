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

import org.springframework.data.redis.connection.RedisSearchCommands;
import org.springframework.util.Assert;

/**
 * Represents a Redis Search query.
 * <p>
 * Use the static factory methods to construct queries:
 * <pre class="code">
 * // Match all documents
 * SearchQuery query = SearchQuery.all();
 *
 * // Raw query string
 * SearchQuery query = SearchQuery.of("@title:hello @status:{active}");
 * </pre>
 * <p>
 * This class is designed for future extension to support a rich query builder API
 * with type-safe predicates for text, numeric, tag, and geo fields.
 *
 * @author Viktoriya Kutsarova
 * @see SearchOptions
 * @see RedisSearchCommands#search(String, SearchQuery)
 * @see <a href="https://redis.io/commands/ft.search/">FT.SEARCH</a>
 */
public class SearchQuery {

	private static final SearchQuery ALL = new SearchQuery("*");

	private final String queryString;

	private SearchQuery(String queryString) {
		this.queryString = queryString;
	}

	/**
	 * Create a {@link SearchQuery} from a raw query string.
	 * <p>
	 * The query string uses Redis Search query syntax. Examples:
	 * <ul>
	 *   <li>{@code "*"} – match all documents</li>
	 *   <li>{@code "hello world"} – full-text search</li>
	 *   <li>{@code "@title:hello"} – field-specific text search</li>
	 *   <li>{@code "@price:[100 500]"} – numeric range</li>
	 *   <li>{@code "@status:{active|pending}"} – tag filter</li>
	 *   <li>{@code "@location:[-122.4 37.7 10 km]"} – geo radius</li>
	 * </ul>
	 *
	 * @param query the raw query string; must not be {@literal null} or empty.
	 * @return a new {@link SearchQuery} instance.
	 * @see <a href="https://redis.io/docs/latest/develop/interact/search-and-query/query/">Redis Search Query Syntax</a>
	 */
	public static SearchQuery of(String query) {
		Assert.hasText(query, "Query must not be null or empty");
		return new SearchQuery(query);
	}

	/**
	 * Create a {@link SearchQuery} that matches all documents in the index.
	 * <p>
	 * Equivalent to the query string {@code "*"}.
	 *
	 * @return a {@link SearchQuery} matching all documents.
	 */
	public static SearchQuery all() {
		return ALL;
	}

	/**
	 * Return the query string in Redis Search syntax.
	 * <p>
	 * This is the string that will be sent to the Redis server as part of the
	 * {@code FT.SEARCH} command.
	 *
	 * @return the query string; never {@literal null}.
	 */
	public String toQueryString() {
		return queryString;
	}

	@Override
	public String toString() {
		return "SearchQuery{" + queryString + "}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SearchQuery that = (SearchQuery) o;
		return queryString.equals(that.queryString);
	}

	@Override
	public int hashCode() {
		return queryString.hashCode();
	}
}

