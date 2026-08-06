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
 * Text-search-specific options ({@link #language}, {@link #languageField}, {@link #termOffsets(boolean)},
 * {@link #highlight(boolean)}, {@link #termFrequencies(boolean)}, {@link #largeSchema(boolean)}) only have effect
 * when the associated {@link Schema} contains at least one {@link TextField}. Validation is performed at
 * index-creation time.
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
	private boolean largeSchema = false;
	private boolean termOffsets = true;
	private @Nullable Long temporary;
	private boolean highlight = true;
	private boolean storeFields = true;
	private boolean termFrequencies = true;
	private @Nullable List<String> stopwords;
	private boolean initialScan = true;

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
	 * Configure whether to encode the index for more than 32 text attributes ({@code MAXTEXTFIELDS}).
	 * <p>
	 * When enabled, allows adding additional text attributes beyond 32 using {@code FT.ALTER}.
	 * For efficiency, RediSearch encodes indexes differently if they are created with less than 32 text attributes.
	 * <p>
	 * Default is {@code false} (optimized encoding for ≤32 text fields).
	 *
	 * @param largeSchema {@code true} to enable large schema support, {@code false} for optimized encoding
	 */
	public IndexDefinition largeSchema(boolean largeSchema) {
		this.largeSchema = largeSchema;
		return this;
	}

	/**
	 * Create a lightweight temporary index that expires after the given seconds of inactivity ({@code TEMPORARY}).
	 * <p>
	 * The internal idle timer is reset whenever the index is searched or added to.
	 * Default is {@code null} (permanent index).
	 *
	 * @param seconds inactivity timeout in seconds
	 */
	public IndexDefinition temporary(long seconds) {
		this.temporary = seconds;
		return this;
	}

	/**
	 * Configure whether to store term offsets ({@code NOOFFSETS} when disabled).
	 * <p>
	 * Term offsets enable exact phrase matching and highlighting. Disabling saves memory.
	 * <p>
	 * Note: Setting to {@code false} also disables highlighting support.
	 * <p>
	 * Default is {@code true} (offsets are stored).
	 *
	 * @param termOffsets {@code true} to store term offsets, {@code false} to disable
	 */
	public IndexDefinition termOffsets(boolean termOffsets) {
		this.termOffsets = termOffsets;
		if (!termOffsets) {
			this.highlight = false;
		}
		return this;
	}

	/**
	 * Configure whether to enable highlighting support ({@code NOHL} when disabled).
	 * <p>
	 * Highlighting allows marking matched terms in search results (e.g., "The &lt;b&gt;quick&lt;/b&gt; brown
	 * &lt;b&gt;fox&lt;/b&gt;"). Disabling saves memory by not storing byte offsets for term positions.
	 * <p>
	 * Default is {@code true} (highlighting is enabled).
	 *
	 * @param highlight {@code true} to enable highlighting, {@code false} to disable
	 */
	public IndexDefinition highlight(boolean highlight) {
		this.highlight = highlight;
		return this;
	}

	/**
	 * Configure whether to store attribute bits per term ({@code NOFIELDS} when disabled).
	 * <p>
	 * When enabled, allows filtering search results by which field matched. Disabling saves memory.
	 * <p>
	 * Default is {@code true} (field information is stored).
	 *
	 * @param storeFields {@code true} to store field information, {@code false} to disable
	 */
	public IndexDefinition storeFields(boolean storeFields) {
		this.storeFields = storeFields;
		return this;
	}

	/**
	 * Configure whether to store term frequencies ({@code NOFREQS} when disabled).
	 * <p>
	 * Term frequency data allows documents with more occurrences of a search term to rank higher.
	 * Disabling saves memory but disables frequency-based relevance ranking.
	 * <p>
	 * Default is {@code true} (frequencies are stored).
	 *
	 * @param termFrequencies {@code true} to store term frequencies, {@code false} to disable
	 */
	public IndexDefinition termFrequencies(boolean termFrequencies) {
		this.termFrequencies = termFrequencies;
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
	 * Configure whether to perform an initial full-database scan when creating the index
	 * ({@code SKIPINITIALSCAN} when disabled).
	 * <p>
	 * When enabled, existing documents matching the index criteria are indexed immediately.
	 * Disabling skips the initial scan, and only newly created/modified documents are indexed.
	 * <p>
	 * Default is {@code true} (initial scan is performed).
	 *
	 * @param initialScan {@code true} to scan existing documents, {@code false} to skip
	 */
	public IndexDefinition initialScan(boolean initialScan) {
		this.initialScan = initialScan;
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

	/**
	 * Return whether large schema support is enabled (more than 32 text fields).
	 */
	public boolean isLargeSchema() {
		return largeSchema;
	}

	/**
	 * Return the temporary index timeout in seconds, or {@literal null} for a permanent index.
	 */
	public @Nullable Long getTemporary() {
		return temporary;
	}

	/**
	 * Return whether term offsets are stored.
	 */
	public boolean isTermOffsets() {
		return termOffsets;
	}

	/**
	 * Return whether highlighting support is enabled.
	 */
	public boolean isHighlight() {
		return highlight;
	}

	/**
	 * Return whether field information is stored per term.
	 */
	public boolean isStoreFields() {
		return storeFields;
	}

	/**
	 * Return whether term frequencies are stored.
	 */
	public boolean isTermFrequencies() {
		return termFrequencies;
	}

	/**
	 * Returns the custom stopword list, or {@literal null} if the default stopwords should be used.
	 * An empty list means stopwords are disabled entirely.
	 */
	public @Nullable List<String> getStopwords() {
		return stopwords != null ? Collections.unmodifiableList(stopwords) : null;
	}

	/**
	 * Return whether an initial scan of existing documents is performed.
	 */
	public boolean isInitialScan() {
		return initialScan;
	}
}
