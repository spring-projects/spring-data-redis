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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * Definition of a Redis Search index, covering all options of the {@code FT.CREATE} command at the index level.
 * <p>
 * Use the fluent factory method {@link #create()} to build an instance:
 * <pre class="code">
 * IndexDefinition def = IndexDefinition.create()
 *     .on(IndexDataType.HASH)
 *     .prefix("doc:")
 *     .language(Language.ENGLISH);
 * </pre>
 * <p>
 * When no {@link #prefix(String...) prefix} is specified, all keys are indexed (equivalent to {@code PREFIX 0 *}).
 * <p>
 * Text-search-specific options ({@link #language}, {@link #languageField}, {@link #noOffsets()},
 * {@link #noHighlighting()}, {@link #noFrequencies()}, {@link #maxTextFields()}) only have effect when the associated
 * {@link Schema} contains at least one {@link TextField}. Validation is performed at index-creation time.
 *
 * @author Viktoriya Kutsarova
 * @see Schema
 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE</a>
 */
public class IndexDefinition {

	private IndexDataType on = IndexDataType.HASH;
	private final List<String> prefixes = new ArrayList<>();
	private @Nullable String filter;
	private @Nullable Language language;
	private @Nullable String languageField;
	private @Nullable Double score;
	private @Nullable String scoreField;
	private @Nullable String payloadField;
	private boolean maxTextFields;
	private boolean noOffsets;
	private @Nullable Long expiresAfterIdleSeconds;
	private boolean noHighlighting;
	private boolean noFields;
	private boolean noFrequencies;
	private @Nullable List<String> stopwords;
	private boolean skipInitialScan;

	private IndexDefinition() {}

	/**
	 * Create a new {@link IndexDefinition}.
	 */
	public static IndexDefinition create() {
		return new IndexDefinition();
	}

	/**
	 * Set the data structure type this index operates on.
	 *
	 * @param dataType {@link IndexDataType#HASH} or {@link IndexDataType#JSON}.
	 */
	public IndexDefinition on(IndexDataType dataType) {
		this.on = dataType;
		return this;
	}

	/**
	 * Add one or more key prefixes to restrict which documents are indexed.
	 *
	 * @param prefixes key prefixes, e.g. {@code "doc:"}.
	 */
	public IndexDefinition prefix(String... prefixes) {
		this.prefixes.addAll(Arrays.asList(prefixes));
		return this;
	}

	/**
	 * Set an aggregation expression filter applied before indexing (e.g. {@code "@status==active"}).
	 */
	public IndexDefinition filter(String filter) {
		this.filter = filter;
		return this;
	}

	/**
	 * Set the default language for text field stemming.
	 *
	 * @param language the language for stemming
	 * @see Language
	 */
	public IndexDefinition language(Language language) {
		this.language = language;
		return this;
	}

	/**
	 * Set the document attribute whose value specifies the per-document language.
	 */
	public IndexDefinition languageField(String languageField) {
		this.languageField = languageField;
		return this;
	}

	/**
	 * Set the default document score (0.0–1.0, default 1.0).
	 */
	public IndexDefinition score(double score) {
		this.score = score;
		return this;
	}

	/**
	 * Set the document attribute used as the document's floating-point score.
	 */
	public IndexDefinition scoreField(String scoreField) {
		this.scoreField = scoreField;
		return this;
	}

	/**
	 * Set the document attribute to use as a binary-safe payload (for custom scoring).
	 */
	public IndexDefinition payloadField(String payloadField) {
		this.payloadField = payloadField;
		return this;
	}

	/**
	 * Enable {@code MAXTEXTFIELDS} — encode the index for more than 32 text attributes.
	 */
	public IndexDefinition maxTextFields() {
		this.maxTextFields = true;
		return this;
	}

	/**
	 * Create a lightweight temporary index that expires after the given seconds of inactivity.
	 * The internal idle timer is reset whenever the index is searched or added to.
	 *
	 * @param seconds inactivity timeout in seconds.
	 */
	public IndexDefinition expiresAfterIdleSeconds(long seconds) {
		this.expiresAfterIdleSeconds = seconds;
		return this;
	}

	/**
	 * Disable term offset storage ({@code NOOFFSETS}). Saves memory; disables exact phrase matching and highlighting.
	 * <p>
	 * Note: This implies {@link #noHighlighting()} — highlighting support is automatically disabled.
	 */
	public IndexDefinition noOffsets() {
		this.noOffsets = true;
		this.noHighlighting = true;
		return this;
	}

	/**
	 * Disable highlighting support ({@code NOHL}). Saves memory by not storing byte offsets for term positions.
	 * Highlighting allows marking matched terms in search results (e.g., "The <b>quick</b> brown <b>fox</b>").
	 */
	public IndexDefinition noHighlighting() {
		this.noHighlighting = true;
		return this;
	}

	/**
	 * Do not store attribute bits per term ({@code NOFIELDS}). Disables per-field filtering during search.
	 */
	public IndexDefinition noFields() {
		this.noFields = true;
		return this;
	}

	/**
	 * Disable term frequency storage ({@code NOFREQS}). Saves memory; disables frequency-based relevance ranking.
	 * Frequency data allows documents with more occurrences of a search term to rank higher.
	 */
	public IndexDefinition noFrequencies() {
		this.noFrequencies = true;
		return this;
	}

	/**
	 * Set a custom stopword list. Pass an empty list to disable all stopwords.
	 *
	 * @param stopwords custom stopword list; empty list disables stopwords entirely.
	 */
	public IndexDefinition stopwords(String... stopwords) {
		this.stopwords = new ArrayList<>(Arrays.asList(stopwords));
		return this;
	}

	/**
	 * Skip the initial full-database scan when creating the index ({@code SKIPINITIALSCAN}).
	 */
	public IndexDefinition skipInitialScan() {
		this.skipInitialScan = true;
		return this;
	}

	// --- Accessors ---

	public IndexDataType getOn() {
		return on;
	}

	public List<String> getPrefixes() {
		return Collections.unmodifiableList(prefixes);
	}

	public @Nullable String getFilter() {
		return filter;
	}

	public @Nullable Language getLanguage() {
		return language;
	}

	public @Nullable String getLanguageField() {
		return languageField;
	}

	public @Nullable Double getScore() {
		return score;
	}

	public @Nullable String getScoreField() {
		return scoreField;
	}

	public @Nullable String getPayloadField() {
		return payloadField;
	}

	public boolean isMaxTextFields() {
		return maxTextFields;
	}

	public @Nullable Long getExpiresAfterIdleSeconds() {
		return expiresAfterIdleSeconds;
	}

	public boolean isNoOffsets() {
		return noOffsets;
	}

	public boolean isNoHighlighting() {
		return noHighlighting;
	}

	public boolean isNoFields() {
		return noFields;
	}

	public boolean isNoFrequencies() {
		return noFrequencies;
	}

	/**
	 * Returns the custom stopword list, or {@literal null} if the default stopwords should be used.
	 * An empty list means stopwords are disabled entirely.
	 */
	public @Nullable List<String> getStopwords() {
		return stopwords != null ? Collections.unmodifiableList(stopwords) : null;
	}

	public boolean isSkipInitialScan() {
		return skipInitialScan;
	}
}
