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

import org.springframework.data.redis.util.BoundedIterator;
import org.springframework.data.util.CloseableIterator;

/**
 * Cursor abstraction to scan over the keyspace or elements within a data structure using a variant of a {@code SCAN}
 * command.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T>
 * @since 1.4
 */
public interface Cursor<T> extends BoundedIterator<T>, CloseableIterator<T> {

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

	/**
	 * Limit the maximum number of elements to be returned from this cursor. The returned cursor object can be used to
	 * iterate over the remaining items and to {@link #close() release} associated resources. The returned cursor is not
	 * attached to the state of {@code this} cursor and this object should be no longer used.
	 *
	 * @return a new {@link Cursor} with detached iteration state.
	 * @since 2.5
	 */
	@Override
	Cursor<T> limit(long count);
}
