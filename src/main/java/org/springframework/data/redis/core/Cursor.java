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

import org.springframework.data.util.CloseableIterator;

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
	 * Get the reference cursor. <br>
	 * <strong>NOTE:</strong> the id might change while iterating items.
	 *
	 * @return
	 */
	long getCursorId();

	/**
	 * @return {@code true} if cursor closed.
	 */
	boolean isClosed();

	/**
	 * Opens cursor and returns itself. This method is intended to be called by components constructing a {@link Cursor}
	 * and should not be called externally.
	 *
	 * @return the opened cursor.
	 * @deprecated to be removed from the interface in the next major version.
	 */
	@Deprecated
	Cursor<T> open();

	/**
	 * @return the current position of the cursor.
	 */
	long getPosition();
}
