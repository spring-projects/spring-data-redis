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

import java.time.Duration;

/**
 * Value object representing a single pending message containing its {@literal ID}, the {@literal consumer} that fetched
 * the message and has still to acknowledge it, the time elapsed since the messages last delivery and the the total
 * number of times delivered.
 * 
 * @author Christoph Strobl
 * @since 2.3
 */
public class PendingMessage {

	private final RecordId id;
	private final Consumer consumer;
	private final Duration elapsedTimeSinceLastDelivery;
	private final Long totalDeliveryCount;

	public PendingMessage(RecordId id, Consumer consumer, Duration elapsedTimeSinceLastDelivery,
			Long totalDeliveryCount) {

		this.id = id;
		this.consumer = consumer;
		this.elapsedTimeSinceLastDelivery = elapsedTimeSinceLastDelivery;
		this.totalDeliveryCount = totalDeliveryCount;
	}

	/**
	 * @return the message id.
	 */
	public RecordId getId() {
		return id;
	}

	/**
	 * @return the message id as {@link String}.
	 */
	public String getStringId() {
		return id.getValue();
	}

	/**
	 * The {@link Consumer} to acknowledge the message.
	 * 
	 * @return never {@literal null}.
	 */
	public Consumer getConsumer() {
		return consumer;
	}

	/**
	 * The {@literal consumer name} to acknowledge the message.
	 *
	 * @return never {@literal null}.
	 */
	public String getConsumerName() {
		return consumer.getName();
	}

	/**
	 * Get the {@literal consumer group}.
	 *
	 * @return never {@literal null}.
	 */
	public String getGroupName() {
		return consumer.getGroup();
	}

	/**
	 * Get the time elapsed since the messages last delivery to the {@link #getConsumer() consumer}.
	 * 
	 * @return never {@literal null}.
	 */
	public Duration getElapsedTimeSinceLastDelivery() {
		return elapsedTimeSinceLastDelivery;
	}

	/**
	 * Get the milliseconds elapsed since the messages last delivery to the {@link #getConsumer() consumer}.
	 *
	 * @return never {@literal null}.
	 */
	public Long getElapsedTimeSinceLastDeliveryMS() {
		return elapsedTimeSinceLastDelivery.toMillis();
	}

	/**
	 * Get the total number of times the messages has been delivered to the {@link #getConsumer() consumer}.
	 * 
	 * @return never {@literal null}.
	 */
	public Long getTotalDeliveryCount() {
		return totalDeliveryCount;
	}

	@Override
	public String toString() {
		return "PendingMessage{" + "id=" + id + ", consumer=" + consumer + ", elapsedTimeSinceLastDeliveryMS="
				+ elapsedTimeSinceLastDelivery.toMillis() + ", totalDeliveryCount=" + totalDeliveryCount + '}';
	}
}
