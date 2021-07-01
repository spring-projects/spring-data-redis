/*
 * Copyright 2014-2021 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Options to be used for with {@literal SCAN} commands.
 *
 * @author Mark Paluch
 * @since 2.6
 */
public class KeyScanOptions extends ScanOptions {

	/**
	 * Constant to apply default {@link KeyScanOptions} without setting a limit or matching a pattern.
	 */
	public static KeyScanOptions NONE = new KeyScanOptions(null, null, null, null);

	private final @Nullable String type;

	private KeyScanOptions(@Nullable Long count, @Nullable String pattern, @Nullable byte[] bytePattern,
			@Nullable String type) {

		super(count, pattern, bytePattern);
		this.type = type;
	}

	/**
	 * Static factory method that returns a new {@link KeyScanOptionsBuilder}.
	 *
	 * @return
	 */
	public static KeyScanOptionsBuilder scanOptions() {
		return new KeyScanOptionsBuilder();
	}

	@Nullable
	public String getType() {
		return type;
	}

	@Override
	public String toOptionString() {

		if (this.equals(KeyScanOptions.NONE)) {
			return "";
		}

		StringJoiner joiner = new StringJoiner(", ").add(super.toOptionString());

		if (StringUtils.hasText(type)) {
			joiner.add("'type' '" + type + "'");
		}

		return joiner.toString();
	}

	public static class KeyScanOptionsBuilder extends ScanOptionsBuilder {

		private @Nullable String type;

		private KeyScanOptionsBuilder() {}

		/**
		 * Returns the current {@link KeyScanOptionsBuilder} configured with the given {@code count}.
		 *
		 * @param count
		 * @return
		 */
		@Override
		public KeyScanOptionsBuilder count(long count) {
			super.count(count);
			return this;
		}

		/**
		 * Returns the current {@link KeyScanOptionsBuilder} configured with the given {@code pattern}.
		 *
		 * @param pattern
		 * @return
		 */
		@Override
		public KeyScanOptionsBuilder match(String pattern) {
			super.match(pattern);
			return this;
		}

		/**
		 * Returns the current {@link KeyScanOptionsBuilder} configured with the given {@code pattern}.
		 *
		 * @param pattern
		 * @return
		 */
		@Override
		public KeyScanOptionsBuilder match(byte[] pattern) {
			super.match(pattern);
			return this;
		}

		/**
		 * Returns the current {@link KeyScanOptionsBuilder} configured with the given {@code type}.
		 *
		 * @param type
		 * @return
		 */
		public KeyScanOptionsBuilder type(String type) {
			this.type = type;
			return this;
		}

		/**
		 * Builds a new {@link KeyScanOptions} objects.
		 *
		 * @return a new {@link KeyScanOptions} objects.
		 */
		@Override
		public KeyScanOptions build() {
			return new KeyScanOptions(count, pattern, bytePattern, type);
		}
	}
}
