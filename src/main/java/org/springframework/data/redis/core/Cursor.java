/*
 * Copyright 2014-2025 the original author or authors.
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

import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;

/**
 * Cursor abstraction to scan over the keyspace or elements within a data structure using a variant of a {@code SCAN}
 * command.
 * <p>
 * Using a Java 8 {@link #stream() java.util.stream.Stream} allows to apply additional
 * {@link java.util.stream.Stream#filter(java.util.function.Predicate) filters} and
 * {@link java.util.stream.Stream#limit(long) limits} to the underlying {@link Cursor}.
 * <p>
 * Make sure to {@link CloseableIterator#close() close} the cursor when done as this allows implementations to clean up
 * any resources they need to keep open to iterate over elements (eg. by using a try-with-resource statement).
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T>
 * @since 1.4
 */
public interface Cursor<T> extends CloseableIterator<T> {

	/**
	 * Returns the reference cursor.
	 *
	 * @return the reference cursor.
	 * @since 3.2.1
	 */
	CursorId getId();

	/**
	 * Get the reference cursor. <br>
	 * <strong>NOTE:</strong> the id might change while iterating items.
	 *
	 * @return
	 * @deprecated since 3.3, use {@link #getId()} instead as the cursorId can exceed {@link Long#MAX_VALUE}.
	 */
	@Deprecated(since = "3.3")
	long getCursorId();

	/**
	 * @return {@code true} if cursor closed.
	 */
	boolean isClosed();

	/**
	 * @return the current position of the cursor.
	 */
	long getPosition();

	/**
	 * Value class representing a cursor identifier.
	 *
	 * @since 3.2.1
	 */
	abstract class CursorId {

		private final static CursorId INITIAL = new CursorId() {
			@Override
			public String getCursorId() {
				return "0";
			}
		};

		/**
		 * Creates a new initial {@link CursorId}.
		 *
		 * @return an initial {@link CursorId}.
		 */
		public static CursorId initial() {
			return INITIAL;
		}

		/**
		 * Creates a {@link CursorId} from the given {@code cursorId}.
		 *
		 * @param cursorId the provided cursor identifier.
		 * @return the provided cursor Id.
		 */
		public static CursorId of(String cursorId) {

			Assert.notNull(cursorId, "CursorId must not be null");

			if (INITIAL.getCursorId().equals(cursorId)) {
				return INITIAL;
			}

			return new CursorId() {

				@Override
				public String getCursorId() {
					return cursorId;
				}
			};
		}

		/**
		 * Creates a {@link CursorId} from the given {@code cursorId}.
		 *
		 * @param cursorId the provided cursor identifier.
		 * @return the provided cursor Id.
		 */
		public static CursorId of(long cursorId) {

			if (cursorId == 0) {
				return INITIAL;
			}
			return of(Long.toUnsignedString(cursorId));
		}

		/**
		 * Returns whether the given {@code cursorId} represent an initial cursor identifier to indicate an initial/finished
		 * cursor state.
		 *
		 * @param cursorId the cursor identifier to inspect.
		 * @return {@code true} if the cursorId represents an initial/finished state.
		 */
		public static boolean isInitial(String cursorId) {
			return INITIAL.getCursorId().equals(cursorId);
		}

		/**
		 * Returns whether the current cursor identifier represent an initial cursor identifier to indicate an
		 * initial/finished cursor state.
		 *
		 * @return {@code true} if the cursorId represents an initial/finished state.
		 */
		public boolean isInitial() {
			return INITIAL.getCursorId().equals(getCursorId());
		}

		/**
		 * @return the raw cursor Id.
		 */
		public abstract String getCursorId();

		@Override
		public int hashCode() {
			return getCursorId().hashCode();
		}

		@Override
		public boolean equals(Object obj) {

			if (obj instanceof CursorId other) {
				return getCursorId().equals(other.getCursorId());
			}

			return false;
		}

		@Override
		public String toString() {
			return getCursorId();
		}

	}
}
