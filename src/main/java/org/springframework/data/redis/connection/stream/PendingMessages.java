/*
 * Copyright 2020 the original author or authors.
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

import org.springframework.data.domain.Range;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Value object holding detailed information about pending messages in {@literal consumer group} for a given
 * {@link org.springframework.data.domain.Range} and offset.
 *
 * @author Christoph Strobl
 * @since 2.3
 */
public class PendingMessages implements Streamable<PendingMessage> {

	private final String groupName;
	private final Range<?> range;
	private final List<PendingMessage> pendingMessages;

	public PendingMessages(String groupName, List<PendingMessage> pendingMessages) {
		this(groupName, Range.unbounded(), pendingMessages);
	}

	public PendingMessages(String groupName, Range<?> range, List<PendingMessage> pendingMessages) {

		Assert.notNull(range, "Range must not be null");
		Assert.notNull(pendingMessages, "Pending Messages must not be null");

		this.groupName = groupName;
		this.range = range;
		this.pendingMessages = pendingMessages;
	}

	/**
	 * Adds the range to the current {@link PendingMessages}.
	 *
	 * @param range must not be {@literal null}.
	 * @return new instance of {@link PendingMessages}.
	 */
	public PendingMessages withinRange(Range<?> range) {
		return new PendingMessages(groupName, range, pendingMessages);
	}

	/**
	 * The {@literal consumer group} name.
	 *
	 * @return never {@literal null}.
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * The {@link Range} pending messages have been loaded.
	 *
	 * @return never {@literal null}.
	 */
	public Range<?> getRange() {
		return range;
	}

	/**
	 * @return {@literal true} if no messages pending within range.
	 */
	public boolean isEmpty() {
		return pendingMessages.isEmpty();
	}

	/**
	 * @return the number of pending messages in range.
	 */
	public int size() {
		return pendingMessages.size();
	}

	/**
	 * Get the {@link PendingMessage} at the given position.
	 *
	 * @param index
	 * @return the {@link PendingMessage} a the given index.
	 * @throws IndexOutOfBoundsException if the index is out of range.
	 */
	public PendingMessage get(int index) {
		return pendingMessages.get(index);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<PendingMessage> iterator() {
		return pendingMessages.iterator();
	}

	@Override
	public String toString() {
		return "PendingMessages{" + "groupName='" + groupName + '\'' + ", range=" + range + ", pendingMessages="
				+ pendingMessages + '}';
	}
}
