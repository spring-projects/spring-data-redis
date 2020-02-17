/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.connection.stream;

import java.util.Collections;
import java.util.Map;

import org.springframework.data.domain.Range;

/**
 * Value Object summarizing pending messages in a {@literal consumer group}. It contains the total number and ID range
 * of pending messages for this consumer group, as well as a collection of total pending messages per consumer.
 * 
 * @since 2.3
 * @author Christoph Strobl
 */
public class PendingMessagesSummary {

	private final String groupName;
	private final Long totalPendingMessages;
	private final Range<String> idRange;
	private final Map<String, Long> pendingMessagesPerConsumer;

	public PendingMessagesSummary(String groupName, Long totalPendingMessages, Range<String> idRange,
			Map<String, Long> pendingMessagesPerConsumer) {

		this.groupName = groupName;
		this.totalPendingMessages = totalPendingMessages;
		this.idRange = idRange;
		this.pendingMessagesPerConsumer = pendingMessagesPerConsumer;
	}

	/**
	 * Get the range between the smallest and greatest ID among the pending messages.
	 *
	 * @return never {@literal null}.
	 */
	public Range<String> getIdRange() {
		return idRange;
	}

	/**
	 * Get the smallest ID among the pending messages.
	 * 
	 * @return never {@literal null}.
	 */
	public RecordId minRecordId() {
		return RecordId.of(minMessageId());
	}

	/**
	 * Get the greatest ID among the pending messages.
	 * 
	 * @return never {@literal null}.
	 */
	public RecordId maxRecordId() {
		return RecordId.of(maxMessageId());
	}

	/**
	 * Get the smallest ID as {@link String} among the pending messages.
	 * 
	 * @return never {@literal null}.
	 */
	public String minMessageId() {
		return idRange.getLowerBound().getValue().get();
	}

	/**
	 * Get the greatest ID as {@link String} among the pending messages.
	 * 
	 * @return never {@literal null}.
	 */
	public String maxMessageId() {
		return idRange.getUpperBound().getValue().get();
	}

	/**
	 * Get the number of total pending messages within the {@literal consumer group}.
	 *
	 * @return
	 */
	public Long getTotalPendingMessages() {
		return totalPendingMessages;
	}

	/**
	 * @return the {@literal consumer group} name.
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * Obtain a map of every {@literal consumer} in the {@literal consumer group} with at least one pending message, and
	 * the number of pending messages.
	 * 
	 * @return never {@literal null}.
	 */
	public Map<String, Long> getPendingMessagesPerConsumer() {
		return Collections.unmodifiableMap(pendingMessagesPerConsumer);
	}

	@Override
	public String toString() {

		return "PendingMessagesSummary{" + "groupName='" + groupName + '\'' + ", totalPendingMessages='"
				+ getTotalPendingMessages() + '\'' + ", minMessageId='" + minMessageId() + '\'' + ", maxMessageId='"
				+ maxMessageId() + '\'' + ", pendingMessagesPerConsumer=" + pendingMessagesPerConsumer + '}';
	}
}
