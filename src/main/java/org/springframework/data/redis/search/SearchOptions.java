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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.redis.connection.RedisSearchCommands;

/**
 * Options for a Redis Search {@code FT.SEARCH} query.
 * <p>
 * Use the fluent factory method {@link #create()} to build an instance:
 * <pre class="code">
 * SearchOptions opts = SearchOptions.create()
 *     .limit(0, 10)
 *     .sortBy("price", Direction.ASC)
 *     .scores(true);
 * </pre>
 *
 * @author Viktoriya Kutsarova
 * @see SearchQuery
 * @see RedisSearchCommands#search(String, SearchQuery, SearchOptions)
 * @see <a href="https://redis.io/commands/ft.search/">FT.SEARCH</a>
 */
public class SearchOptions {

	/**
	 * Default value for the maximum number of results to return when using {@link #limit(long, long)}.
	 * This value is applied when no explicit limit is configured.
	 */
	public static final long DEFAULT_LIMIT_COUNT = 10000;

	private long limitOffset = 0;
	private long limitCount = DEFAULT_LIMIT_COUNT;
	private @Nullable String sortBy;
	private Direction sortDirection = Direction.ASC;
	private boolean includeContent = true;
	private boolean verbatim = false;
	private boolean scores = false;
	private boolean sortKeys = false;
	private boolean inOrder = false;
	private boolean explainScore = false;
	private @Nullable List<String> returnFields;
	private @Nullable String language;
	private @Nullable Integer slop;
	private @Nullable Long timeout;
	private @Nullable String expander;
	private @Nullable String scorer;
	private @Nullable List<String> inKeys;
	private @Nullable List<String> inFields;

	private SearchOptions() {}

	/**
	 * Create a new {@link SearchOptions} builder.
	 */
	public static SearchOptions create() {
		return new SearchOptions();
	}

	/**
	 * Limit the number of results returned ({@code LIMIT offset count}).
	 *
	 * @param offset zero-based starting position.
	 * @param count maximum number of results to return.
	 */
	public SearchOptions limit(long offset, long count) {
		this.limitOffset = offset;
		this.limitCount = count;
		return this;
	}

	/**
	 * Limit the number of results returned using a {@link Pageable} ({@code LIMIT offset count}).
	 *
	 * @param pageable the pageable to extract offset and page size from.
	 */
	public SearchOptions limit(Pageable pageable) {
		this.limitOffset = pageable.getOffset();
		this.limitCount = pageable.getPageSize();
		return this;
	}

	/**
	 * Sort results by the given field ({@code SORTBY field ASC}).
	 *
	 * @param field the field name to sort by.
	 */
	public SearchOptions sortBy(String field) {
		return sortBy(field, Direction.ASC);
	}

	/**
	 * Sort results by the given field and direction ({@code SORTBY field direction}).
	 *
	 * @param field the field name to sort by.
	 * @param direction {@link Direction#ASC} or {@link Direction#DESC}.
	 */
	public SearchOptions sortBy(String field, Direction direction) {
		this.sortBy = field;
		this.sortDirection = direction;
		return this;
	}

	/**
	 * Configure whether to include document field content in the results ({@code NOCONTENT} when disabled).
	 * <p>
	 * When set to {@code false}, only document ids are returned without their field content.
	 * <p>
	 * Default is {@code true} (content is included).
	 *
	 * @param includeContent {@code true} to include content, {@code false} to return only document ids
	 */
	public SearchOptions includeContent(boolean includeContent) {
		this.includeContent = includeContent;
		return this;
	}

	/**
	 * Configure whether to include the relevance score for each returned document ({@code WITHSCORES}).
	 * <p>
	 * Default is {@code false} (scores are not included).
	 *
	 * @param scores {@code true} to include relevance scores, {@code false} to omit
	 */
	public SearchOptions scores(boolean scores) {
		this.scores = scores;
		return this;
	}

	/**
	 * Configure whether to include the sort key for each returned document ({@code WITHSORTKEYS}).
	 * <p>
	 * Default is {@code false} (sort keys are not included).
	 *
	 * @param sortKeys {@code true} to include sort keys, {@code false} to omit
	 */
	public SearchOptions sortKeys(boolean sortKeys) {
		this.sortKeys = sortKeys;
		return this;
	}

	/**
	 * Restrict the fields returned in each document ({@code RETURN}).
	 *
	 * @param fields field names to include in the results.
	 */
	public SearchOptions returnFields(String... fields) {
		this.returnFields = Arrays.asList(fields);
		return this;
	}

	/**
	 * Override the default stemming language for this query ({@code LANGUAGE}).
	 *
	 * @param language BCP-47 language code, e.g. {@code "english"}, {@code "chinese"}.
	 */
	public SearchOptions language(String language) {
		this.language = language;
		return this;
	}

	/**
	 * Do not try to use stemming for query expansion ({@code VERBATIM}).
	 * <p>
	 * When enabled, the query terms are matched exactly as provided without stemming.
	 */
	public SearchOptions verbatim() {
		this.verbatim = true;
		return this;
	}

	/**
	 * Require the query terms to appear in order in the document ({@code INORDER}).
	 * <p>
	 * This is typically used together with {@link #slop(int)} to control phrase matching.
	 */
	public SearchOptions inOrder() {
		this.inOrder = true;
		return this;
	}

	/**
	 * Return an explanation of the scoring for each result ({@code EXPLAINSCORE}).
	 * <p>
	 * This is useful for debugging relevance scoring. Requires {@link #scores(boolean) scores(true)} to be set.
	 */
	public SearchOptions explainScore() {
		this.explainScore = true;
		return this;
	}

	/**
	 * Set the maximum allowed distance (slop) between query terms ({@code SLOP slop}).
	 * <p>
	 * A slop of 0 means exact phrase match. Higher values allow more words between terms.
	 *
	 * @param slop the maximum number of intervening non-matched terms; must be non-negative.
	 */
	public SearchOptions slop(int slop) {
		if (slop < 0) {
			throw new IllegalArgumentException("Slop must be non-negative");
		}
		this.slop = slop;
		return this;
	}

	/**
	 * Set the query timeout in milliseconds ({@code TIMEOUT timeout}).
	 * <p>
	 * If the query exceeds this timeout, it will be terminated.
	 *
	 * @param timeoutMillis the timeout in milliseconds; must be positive.
	 */
	public SearchOptions timeout(long timeoutMillis) {
		if (timeoutMillis <= 0) {
			throw new IllegalArgumentException("Timeout must be positive");
		}
		this.timeout = timeoutMillis;
		return this;
	}

	/**
	 * Use a custom query expander instead of the default stemmer ({@code EXPANDER expander}).
	 *
	 * @param expander the name of the custom expander.
	 */
	public SearchOptions expander(String expander) {
		this.expander = expander;
		return this;
	}

	/**
	 * Use a custom scoring function ({@code SCORER scorer}).
	 *
	 * @param scorer the name of the custom scorer.
	 */
	public SearchOptions scorer(String scorer) {
		this.scorer = scorer;
		return this;
	}

	/**
	 * Limit the search to specific document keys ({@code INKEYS count key ...}).
	 *
	 * @param keys the document keys to search within.
	 */
	public SearchOptions inKeys(String... keys) {
		this.inKeys = Arrays.asList(keys);
		return this;
	}

	/**
	 * Limit the search to specific fields ({@code INFIELDS count field ...}).
	 * <p>
	 * By default, the query searches all TEXT fields. Use this to restrict to specific fields.
	 *
	 * @param fields the field names to search within.
	 */
	public SearchOptions inFields(String... fields) {
		this.inFields = Arrays.asList(fields);
		return this;
	}

	// --- Accessors ---

	public long getLimitOffset() {
		return limitOffset;
	}

	public long getLimitCount() {
		return limitCount;
	}


	public @Nullable String getSortBy() {
		return sortBy;
	}

	public Direction getSortDirection() {
		return sortDirection;
	}

	/**
	 * Return whether document content should be included in results.
	 */
	public boolean isIncludeContent() {
		return includeContent;
	}

	/**
	 * Return whether relevance scores should be included.
	 */
	public boolean isScores() {
		return scores;
	}

	/**
	 * Return whether sort keys should be included.
	 */
	public boolean isSortKeys() {
		return sortKeys;
	}

	public boolean isVerbatim() {
		return verbatim;
	}

	public boolean isInOrder() {
		return inOrder;
	}

	public boolean isExplainScore() {
		return explainScore;
	}

	public @Nullable Integer getSlop() {
		return slop;
	}

	public @Nullable Long getTimeout() {
		return timeout;
	}

	public @Nullable String getExpander() {
		return expander;
	}

	public @Nullable String getScorer() {
		return scorer;
	}

	/**
	 * Returns the list of keys to search within, or {@literal null} if all keys should be searched.
	 */
	public @Nullable List<String> getInKeys() {
		return inKeys != null ? Collections.unmodifiableList(inKeys) : null;
	}

	/**
	 * Returns the list of fields to search within, or {@literal null} if all fields should be searched.
	 */
	public @Nullable List<String> getInFields() {
		return inFields != null ? Collections.unmodifiableList(inFields) : null;
	}

	/**
	 * Returns the list of fields to return, or {@literal null} if all fields should be returned.
	 */
	public @Nullable List<String> getReturnFields() {
		return returnFields != null ? Collections.unmodifiableList(returnFields) : null;
	}

	public @Nullable String getLanguage() {
		return language;
	}
}
