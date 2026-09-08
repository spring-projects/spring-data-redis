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
package org.springframework.data.redis.connection.stream;

import java.util.Iterator;
import java.util.List;

import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Value object holding the result of an {@literal XAUTOCLAIM ... JUSTID} call: the {@link RecordId cursor} to use as
 * start argument for the next call along with the {@link RecordId ids} of the records whose ownership has been
 * transferred.
 * <p>
 * A cursor of {@code 0-0} indicates that the scan of the pending entries list has been completed.
 *
 * @author big-cir
 * @since 4.2
 * @see <a href="https://redis.io/commands/xautoclaim">Redis Documentation: XAUTOCLAIM</a>
 */
public class ClaimedRecordIds implements Streamable<RecordId> {

	private final RecordId cursor;
	private final List<RecordId> ids;

	/**
	 * Create new {@link ClaimedRecordIds}.
	 *
	 * @param cursor the {@link RecordId} to be used as start argument for the next call. Must not be {@literal null}.
	 * @param ids the ids of the claimed records. Must not be {@literal null}.
	 */
	public ClaimedRecordIds(RecordId cursor, List<RecordId> ids) {

		Assert.notNull(cursor, "Cursor must not be null");
		Assert.notNull(ids, "Ids must not be null");

		this.cursor = cursor;
		this.ids = ids;
	}

	/**
	 * The {@link RecordId} to be used as the start argument for the next {@literal XAUTOCLAIM} call. {@code 0-0}
	 * indicates that the scan is complete.
	 *
	 * @return never {@literal null}.
	 */
	public RecordId getCursor() {
		return cursor;
	}

	/**
	 * The {@link RecordId ids} of the records that were claimed.
	 *
	 * @return never {@literal null}.
	 */
	public List<RecordId> getIds() {
		return ids;
	}

	/**
	 * @return {@literal true} if no records were claimed.
	 */
	public boolean isEmpty() {
		return ids.isEmpty();
	}

	/**
	 * @return the number of claimed records.
	 */
	public int size() {
		return ids.size();
	}

	/**
	 * Get the {@link RecordId} at the given position.
	 *
	 * @param index
	 * @return the {@link RecordId} at the given index.
	 * @throws IndexOutOfBoundsException if the index is out of range.
	 */
	public RecordId get(int index) {
		return ids.get(index);
	}

	@Override
	public Iterator<RecordId> iterator() {
		return ids.iterator();
	}

	@Override
	public String toString() {
		return "ClaimedRecordIds{" + "cursor=" + cursor + ", ids=" + ids + '}';
	}
}
