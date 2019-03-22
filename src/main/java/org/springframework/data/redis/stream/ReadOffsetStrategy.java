/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.redis.stream;

import java.util.Optional;

import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;

/**
 * Strategy to determine the first and subsequent {@link ReadOffset}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
enum ReadOffsetStrategy {

	/**
	 * Use the last seen message Id.
	 */
	NextMessage {
		@Override
		public ReadOffset getFirst(ReadOffset readOffset, Optional<Consumer> consumer) {
			return readOffset;
		}

		@Override
		public ReadOffset getNext(ReadOffset readOffset, Optional<Consumer> consumer, String lastConsumedMessageId) {
			return ReadOffset.from(lastConsumedMessageId);
		}
	},

	/**
	 * Last consumed strategy.
	 */
	LastConsumed {
		@Override
		public ReadOffset getFirst(ReadOffset readOffset, Optional<Consumer> consumer) {
			return consumer.map(it -> ReadOffset.lastConsumed()).orElseGet(ReadOffset::latest);
		}

		@Override
		public ReadOffset getNext(ReadOffset readOffset, Optional<Consumer> consumer, String lastConsumedMessageId) {
			return consumer.map(it -> ReadOffset.lastConsumed()).orElseGet(() -> ReadOffset.from(lastConsumedMessageId));
		}
	},

	/**
	 * Use always the latest stream message.
	 */
	Latest {
		@Override
		public ReadOffset getFirst(ReadOffset readOffset, Optional<Consumer> consumer) {
			return ReadOffset.latest();
		}

		@Override
		public ReadOffset getNext(ReadOffset readOffset, Optional<Consumer> consumer, String lastConsumedMessageId) {
			return ReadOffset.latest();
		}
	};

	/**
	 * Return a {@link ReadOffsetStrategy} given the initial {@link ReadOffset}.
	 *
	 * @param offset must not be {@literal null}.
	 * @return the {@link ReadOffsetStrategy}.
	 */
	static ReadOffsetStrategy getStrategy(ReadOffset offset) {

		if (ReadOffset.latest().equals(offset)) {
			return Latest;
		}

		if (ReadOffset.lastConsumed().equals(offset)) {
			return LastConsumed;
		}

		return NextMessage;
	}

	/**
	 * Determine the first {@link ReadOffset}.
	 *
	 * @param readOffset
	 * @param consumer
	 * @return
	 */
	public abstract ReadOffset getFirst(ReadOffset readOffset, Optional<Consumer> consumer);

	/**
	 * Determine the next {@link ReadOffset} given {@code lastConsumedMessageId}.
	 *
	 * @param readOffset
	 * @param consumer
	 * @param lastConsumedMessageId
	 * @return
	 */
	public abstract ReadOffset getNext(ReadOffset readOffset, Optional<Consumer> consumer, String lastConsumedMessageId);
}
