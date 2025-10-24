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

import static org.springframework.data.redis.core.Cursor.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.springframework.lang.Nullable;

/**
 * {@link ScanIteration} holds the values contained in Redis {@literal Multibulk reply} on exectuting {@literal SCAN}
 * command.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
public class ScanIteration<T> implements Iterable<T> {

	private final CursorId cursorId;
	private final Collection<T> items;

	/**
	 * @param cursorId
	 * @param items
	 * @deprecated since 3.3.0, use {@link ScanIteration#ScanIteration(CursorId, Collection)} instead as {@code cursorId}
	 *             can exceed {@link Long#MAX_VALUE}.
	 */
	@Deprecated(since = "3.3.0")
	public ScanIteration(long cursorId, @Nullable Collection<T> items) {
		this(CursorId.of(cursorId), items);
	}

	/**
	 * @param cursorId
	 * @param items
	 * @since 3.3.0
	 */
	public ScanIteration(CursorId cursorId, @Nullable Collection<T> items) {

		this.cursorId = cursorId;
		this.items = (items != null ? new ArrayList<>(items) : Collections.emptyList());
	}

	/**
	 * The cursor id to be used for subsequent requests.
	 *
	 * @return
	 * @deprecated since 3.3.0, use {@link #getId()} instead as the cursorId can exceed {@link Long#MAX_VALUE}.
	 */
	@Deprecated(since = "3.3.3")
	public long getCursorId() {
		return Long.parseLong(getId().getCursorId());
	}

	/**
	 * The cursor id to be used for subsequent requests.
	 *
	 * @return
	 * @since 3.3.0
	 */
	public CursorId getId() {
		return cursorId;
	}

	/**
	 * Get the items returned.
	 *
	 * @return
	 */
	public Collection<T> getItems() {
		return items;
	}

	@Override
	public Iterator<T> iterator() {
		return items.iterator();
	}

}
