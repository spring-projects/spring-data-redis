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
 * A single document entry in a Redis Search {@code FT.SEARCH} result.
 * <p>
 * The document id is always present. The score is only populated when the corresponding
 * {@link SearchOptions} flag ({@code WITHSCORES}) is specified on the search.
 * The {@link SearchDocument} holds the returned field values and is empty when {@code NOCONTENT} is set.
 *
 * @author Viktoriya Kutsarova
 * @see SearchDocument
 * @see SearchResult
 * @see SearchOptions
 */
public class SearchHit {

	private final String id;
	private final @Nullable Double score;
	private final SearchDocument document;

	/**
	 * Create a new {@link SearchHit}.
	 *
	 * @param id the Redis key that identifies the document; never {@literal null}.
	 * @param score relevance score, present only when {@code WITHSCORES} was requested.
	 * @param document the returned field values; never {@literal null}.
	 */
	public SearchHit(String id, @Nullable Double score, SearchDocument document) {
		this.id = id;
		this.score = score;
		this.document = document;
	}

	/**
	 * Return the document identifier (the Redis key).
	 */
	public String getId() {
		return id;
	}

	/**
	 * Return the relevance score, or {@literal null} if {@code WITHSCORES} was not requested.
	 */
	public @Nullable Double getScore() {
		return score;
	}

	/**
	 * Return the decoded field values for this document.
	 */
	public SearchDocument getDocument() {
		return document;
	}

	@Override
	public String toString() {
		return "SearchHit{id='" + id + "', score=" + score + ", document=" + document + "}";
	}
}
