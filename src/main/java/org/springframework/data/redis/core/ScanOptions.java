/*
 * Copyright 2014-2023 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.StringJoiner;

import org.springframework.data.redis.connection.DataType;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Options to be used for with {@literal SCAN} commands.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.4
 * @see KeyScanOptions
 */
public class ScanOptions {

	/**
	 * Constant to apply default {@link ScanOptions} without setting a limit or matching a pattern.
	 */
	public static ScanOptions NONE = new ScanOptions(null, null, null);

	private final @Nullable Long count;
	private final @Nullable String pattern;
	private final @Nullable byte[] bytePattern;

	ScanOptions(@Nullable Long count, @Nullable String pattern, @Nullable byte[] bytePattern) {

		this.count = count;
		this.pattern = pattern;
		this.bytePattern = bytePattern;
	}

	/**
	 * Static factory method that returns a new {@link ScanOptionsBuilder}.
	 *
	 * @return
	 */
	public static ScanOptionsBuilder scanOptions() {
		return new ScanOptionsBuilder();
	}

	@Nullable
	public Long getCount() {
		return count;
	}

	@Nullable
	public String getPattern() {

		if (bytePattern != null && pattern == null) {
			return new String(bytePattern);
		}

		return pattern;
	}

	@Nullable
	public byte[] getBytePattern() {

		if (bytePattern == null && pattern != null) {
			return pattern.getBytes();
		}

		return bytePattern;
	}

	public String toOptionString() {

		if (this.equals(ScanOptions.NONE)) {
			return "";
		}

		StringJoiner joiner = new StringJoiner(", ");

		if (this.getCount() != null) {
			joiner.add("'count' " + this.getCount());
		}

		String pattern = getPattern();
		if (StringUtils.hasText(pattern)) {
			joiner.add("'match' '" + pattern + "'");
		}

		return joiner.toString();
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.4
	 */
	public static class ScanOptionsBuilder {

		@Nullable Long count;
		@Nullable String pattern;
		@Nullable byte[] bytePattern;
		@Nullable DataType type;

		ScanOptionsBuilder() {}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code count}.
		 *
		 * @param count
		 * @return this.
		 */
		public ScanOptionsBuilder count(long count) {
			this.count = count;
			return this;
		}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code pattern}.
		 *
		 * @param pattern
		 * @return this.
		 */
		public ScanOptionsBuilder match(String pattern) {
			this.pattern = pattern;
			return this;
		}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code pattern}.
		 *
		 * @param pattern
		 * @return this.
		 * @since 2.6
		 */
		public ScanOptionsBuilder match(byte[] pattern) {
			this.bytePattern = pattern;
			return this;
		}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code type}. <br />
		 * Please verify the the targeted command supports the
		 * <a href="https://redis.io/commands/SCAN#the-type-option">TYPE</a> option before use.
		 *
		 * @param type must not be {@literal null}. Either do not set or use {@link DataType#NONE}.
		 * @return this.
		 * @since 2.6
		 */
		public ScanOptionsBuilder type(DataType type) {

			Assert.notNull(type, "Type must not be null! Use NONE instead.");

			this.type = type;
			return this;
		}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code type}.
		 *
		 * @param type the textual representation of {@link DataType#fromCode(String)}. Must not be {@literal null}.
		 * @return this.
		 * @throws IllegalArgumentException if given type is {@literal null} or unknown.
		 */
		public ScanOptionsBuilder type(String type) {

			Assert.notNull(type, "Type must not be null!");
			return type(DataType.fromCode(type));
		}

		/**
		 * Builds a new {@link ScanOptions} objects.
		 *
		 * @return a new {@link ScanOptions} objects.
		 */
		public ScanOptions build() {

			if (type != null && !DataType.NONE.equals(type)) {
				return new KeyScanOptions(count, pattern, bytePattern, type.code());
			}
			return new ScanOptions(count, pattern, bytePattern);
		}
	}
}
