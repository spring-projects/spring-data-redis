/*
 * Copyright 2014-present the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.Cursor.CursorId;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Options to be used for with {@literal SCAN} commands.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Lee Taeho
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
	private final byte @Nullable [] bytePattern;
	private final CursorId cursorId;

	ScanOptions(@Nullable Long count, @Nullable String pattern, byte @Nullable [] bytePattern) {
		this(count, pattern, bytePattern, CursorId.initial());
	}

	ScanOptions(@Nullable Long count, @Nullable String pattern, byte @Nullable [] bytePattern, CursorId cursorId) {

		this.count = count;
		this.pattern = pattern;
		this.bytePattern = bytePattern;
		this.cursorId = cursorId;
	}

	/**
	 * Static factory method that returns a new {@link ScanOptionsBuilder}.
	 *
	 * @return
	 */
	public static ScanOptionsBuilder scanOptions() {
		return new ScanOptionsBuilder();
	}

	public @Nullable Long getCount() {
		return count;
	}

	public @Nullable String getPattern() {

		if (bytePattern != null && pattern == null) {
			return ByteUtils.toString(bytePattern);
		}

		return pattern;
	}

	public byte @Nullable [] getBytePattern() {

		if (bytePattern == null && pattern != null) {
			return pattern.getBytes();
		}

		return bytePattern;
	}

	/**
	 * Returns the cursor to start scanning from. Defaults to {@link CursorId#initial()}.
	 *
	 * @return the cursor to start scanning from.
	 * @since 4.2
	 */
	public CursorId getCursorId() {
		return cursorId;
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
		byte @Nullable [] bytePattern;
		@Nullable DataType type;
		CursorId cursorId = CursorId.initial();

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
		 * Please verify the targeted command supports the
		 * <a href="https://redis.io/commands/SCAN#the-type-option">TYPE</a> option before use.
		 *
		 * @param type must not be {@literal null}. Either do not set or use {@link DataType#NONE}.
		 * @return this.
		 * @since 2.6
		 */
		public ScanOptionsBuilder type(DataType type) {

			Assert.notNull(type, "Type must not be null Use NONE instead");

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

			Assert.notNull(type, "Type must not be null");
			return type(DataType.fromCode(type));
		}

		/**
		 * Returns the current {@link ScanOptionsBuilder} configured with the given {@code cursorId} to start scanning from.
		 * Use the {@link Cursor#getId() id} of a previous {@link Cursor} to continue a scan with the next {@code SCAN}
		 * iteration.
		 * <p>
		 * Note that {@link Cursor#getId()} is the cursor returned by the server for the next iteration. It moves forward as
		 * soon as a batch of elements is fetched, and {@link Cursor#hasNext()} may fetch the next batch in advance.
		 * Elements of the current batch that were not consumed yet are not returned when resuming from that id. Consume the
		 * current batch completely before keeping the id. Elements may be returned more than once, as with any
		 * {@code SCAN}. Once the scan has finished, the id is {@link CursorId#initial()} again and resuming from it starts
		 * a new scan.
		 * <p>
		 * Starting from a cursor other than {@link CursorId#initial()} is not supported (and rejected with
		 * {@link org.springframework.dao.InvalidDataAccessApiUsageException}) when scanning the whole keyspace across a
		 * Redis Cluster and by reactive scan commands.
		 *
		 * @param cursorId must not be {@literal null}.
		 * @return this.
		 * @since 4.2
		 */
		public ScanOptionsBuilder cursorId(CursorId cursorId) {

			Assert.notNull(cursorId, "CursorId must not be null");

			this.cursorId = cursorId;
			return this;
		}

		/**
		 * Builds a new {@link ScanOptions} objects.
		 *
		 * @return a new {@link ScanOptions} objects.
		 */
		public ScanOptions build() {

			if (type != null && !DataType.NONE.equals(type)) {
				return new KeyScanOptions(count, pattern, bytePattern, type.code(), cursorId);
			}
			return new ScanOptions(count, pattern, bytePattern, cursorId);
		}

	}

}
