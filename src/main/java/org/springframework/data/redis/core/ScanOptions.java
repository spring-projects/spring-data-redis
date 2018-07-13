/*
 * Copyright 2014-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Options to be used for with {@literal SCAN} commands.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.4
 */
public class ScanOptions {

	/**
	 * Constant to apply default {@link ScanOptions} without setting a limit or matching a pattern.
	 */
	public static ScanOptions NONE = new ScanOptions(null, null);

	private final @Nullable Long count;
	private final @Nullable String pattern;

	private ScanOptions(@Nullable Long count, @Nullable String pattern) {
		this.count = count;
		this.pattern = pattern;
	}

	/**
	 * Static factory method that returns a new {@link ScanOptionsBuilder}.
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
		return pattern;
	}

	public String toOptionString() {

		if (this.equals(ScanOptions.NONE)) {
			return "";
		}

		String params = "";

		if (this.count != null) {
			params += (", 'count', " + count);
		}
		if (StringUtils.hasText(this.pattern)) {
			params += (", 'match' , '" + this.pattern + "'");
		}

		return params;
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 1.4
	 */
	public static class ScanOptionsBuilder {

		private @Nullable Long count;
		private @Nullable String pattern;


		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code count}.
		 *
		 * @param count
		 * @return
		 */
		public ScanOptionsBuilder count(long count) {
			this.count = count;
			return this;
		}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code pattern}.
		 *
		 * @param pattern
		 * @return
		 */
		public ScanOptionsBuilder match(String pattern) {
			this.pattern = pattern;
			return this;
		}

		/**
		 * Builds a new {@link ScanOptions} objects.
		 *
		 * @return a new {@link ScanOptions} objects.
		 */
		public ScanOptions build() {
			return new ScanOptions(count, pattern);
		}
	}
}
