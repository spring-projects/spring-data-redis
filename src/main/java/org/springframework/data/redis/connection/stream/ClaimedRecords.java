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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Value object holding the result of an {@literal XAUTOCLAIM} call: the {@link RecordId cursor} to use as start
 * argument for the next call along with the {@link Record records} whose ownership has been transferred.
 * <p>
 * A cursor of {@code 0-0} indicates that the scan of the pending entries list has been completed.
 *
 * @param <R> the {@link Record} type.
 * @author big-cir
 * @since 4.2
 * @see <a href="https://redis.io/commands/xautoclaim">Redis Documentation: XAUTOCLAIM</a>
 */
public class ClaimedRecords<R extends Record<?, ?>> implements Streamable<R> {

	private final RecordId cursor;
	private final List<R> records;

	/**
	 * Create new {@link ClaimedRecords}.
	 *
	 * @param cursor the {@link RecordId} to be used as start argument for the next call. Must not be {@literal null}.
	 * @param records the claimed records. Must not be {@literal null}.
	 */
	public ClaimedRecords(RecordId cursor, List<R> records) {

		Assert.notNull(cursor, "Cursor must not be null");
		Assert.notNull(records, "Records must not be null");

		this.cursor = cursor;
		this.records = records;
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
	 * The {@link Record records} that were claimed.
	 *
	 * @return never {@literal null}.
	 */
	public List<R> getRecords() {
		return records;
	}

	/**
	 * @return {@literal true} if no records were claimed.
	 */
	public boolean isEmpty() {
		return records.isEmpty();
	}

	/**
	 * @return the number of claimed records.
	 */
	public int size() {
		return records.size();
	}

	/**
	 * Get the {@link Record} at the given position.
	 *
	 * @param index
	 * @return the {@link Record} at the given index.
	 * @throws IndexOutOfBoundsException if the index is out of range.
	 */
	public R get(int index) {
		return records.get(index);
	}

	/**
	 * Create a new {@link ClaimedRecords} instance by applying the given {@link Function mapper} to each record while
	 * retaining the cursor.
	 *
	 * @param mapper must not be {@literal null}.
	 * @param <T> the target {@link Record} type.
	 * @return new instance of {@link ClaimedRecords}.
	 */
	public <T extends Record<?, ?>> ClaimedRecords<T> mapRecords(Function<? super R, ? extends T> mapper) {

		Assert.notNull(mapper, "Mapping function must not be null");

		List<T> mapped = new ArrayList<>(records.size());
		for (R record : records) {
			mapped.add(mapper.apply(record));
		}

		return new ClaimedRecords<>(cursor, mapped);
	}

	@Override
	public Iterator<R> iterator() {
		return records.iterator();
	}

	@Override
	public String toString() {
		return "ClaimedRecords{" + "cursor=" + cursor + ", records=" + records + '}';
	}
}
