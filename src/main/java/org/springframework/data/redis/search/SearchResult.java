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

import java.util.List;

/**
 * The result of a Redis Search {@code FT.SEARCH} query.
 * <p>
 * {@link #getTotalResults()} reflects the total count reported by the server and may be larger than
 * {@link #getHits()} when a {@code LIMIT} was applied. Each {@link SearchHit} holds the document id
 * and, depending on the {@link SearchOptions} flags used, an optional score, payload, sort key, and
 * field values.
 *
 * @author Viktoriya Kutsarova
 * @see SearchHit
 * @see SearchOptions
 * @see RedisSearchCommands#search(String, SearchQuery)
 */
public class SearchResult {

	private final long totalResults;
	private final List<SearchHit> hits;

	/**
	 * Create a new {@link SearchResult}.
	 *
	 * @param totalResults total number of matching documents in the index (may be larger than {@code hits.size()}).
	 * @param hits the document hits returned in this result page; never {@literal null}.
	 */
	public SearchResult(long totalResults, List<SearchHit> hits) {
		this.totalResults = totalResults;
		this.hits = List.copyOf(hits);
	}

	/**
	 * Return the total number of documents that matched the query in the index.
	 * This may be larger than the actual size of {@link #getHits()} when a {@code LIMIT} was applied.
	 */
	public long getTotalResults() {
		return totalResults;
	}

	/**
	 * Return the document hits returned for this page of results.
	 */
	public List<SearchHit> getHits() {
		return hits;
	}

	@Override
	public String toString() {
		return "SearchResult{totalResults=" + totalResults + ", hits=" + hits + "}";
	}
}
