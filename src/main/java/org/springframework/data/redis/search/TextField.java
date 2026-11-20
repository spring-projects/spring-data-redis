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
 * A full-text {@link SchemaField} supporting stemming, phonetics, and weighted scoring.
 * <p>
 * Create instances via {@link SchemaField#text(String)}.
 *
 * @author Viktoriya Kutsarova
 * @see SchemaField#text(String)
 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE — TEXT field options</a>
 */
public final class TextField extends TextualSchemaField<TextField> {

	private boolean stemming = true;
	private @Nullable PhoneticMatcher phonetic;
	private double weight = 1.0;

	TextField(String name) {
		super(name);
	}

	/**
	 * Configure whether stemming is enabled for this field ({@code NOSTEM} when disabled).
	 * <p>
	 * Stemming allows matching word variations (e.g., "running" matches "run").
	 * <p>
	 * Default is {@code true} (stemming is enabled).
	 *
	 * @param stemming {@code true} to enable stemming, {@code false} to match exact words only
	 */
	public TextField stemming(boolean stemming) {
		this.stemming = stemming;
		return this;
	}

	/**
	 * Enable phonetic matching using the given matcher ({@code PHONETIC}).
	 *
	 * @param matcher the phonetic matching algorithm to use
	 * @see PhoneticMatcher
	 */
	public TextField phonetic(PhoneticMatcher matcher) {
		this.phonetic = matcher;
		return this;
	}

	/**
	 * Set the relevance weight for this field ({@code WEIGHT}). Default is {@code 1.0}.
	 *
	 * @param weight relevance multiplier; higher values increase ranking priority.
	 */
	public TextField weight(double weight) {
		this.weight = weight;
		return this;
	}

	/**
	 * Return whether stemming is enabled for this field.
	 */
	public boolean isStemming() {
		return stemming;
	}

	public @Nullable PhoneticMatcher getPhonetic() {
		return phonetic;
	}

	public double getWeight() {
		return weight;
	}
}
