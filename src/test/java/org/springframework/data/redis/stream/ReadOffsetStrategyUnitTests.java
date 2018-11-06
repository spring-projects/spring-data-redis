/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.stream;

import static org.assertj.core.api.Assertions.*;

import java.util.Optional;

import org.junit.Test;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;

/**
 * Unit tests for {@link ReadOffsetStrategy}.
 *
 * @author Mark Paluch
 */
public class ReadOffsetStrategyUnitTests {

	static Optional<Consumer> consumer = Optional.of(Consumer.from("foo", "bar"));

	@Test // DATAREDIS-864
	public void nextMessageStandaloneShouldReturnLastSeenMessageId() {

		ReadOffset offset = ReadOffset.from("foo");

		assertThat(ReadOffsetStrategy.NextMessage.getFirst(offset, Optional.empty())).isEqualTo(offset);
		assertThat(ReadOffsetStrategy.NextMessage.getNext(offset, Optional.empty(), "42")).isEqualTo(ReadOffset.from("42"));
	}

	@Test // DATAREDIS-864
	public void lastConsumedStandaloneShouldReturnLastSeenMessageId() {

		ReadOffset offset = ReadOffset.lastConsumed();

		assertThat(ReadOffsetStrategy.LastConsumed.getFirst(offset, Optional.empty())).isEqualTo(ReadOffset.latest());
		assertThat(ReadOffsetStrategy.LastConsumed.getNext(offset, Optional.empty(), "42"))
				.isEqualTo(ReadOffset.from("42"));
	}

	@Test // DATAREDIS-864
	public void latestStandaloneShouldReturnLatest() {

		ReadOffset offset = ReadOffset.latest();

		assertThat(ReadOffsetStrategy.Latest.getFirst(offset, Optional.empty())).isEqualTo(ReadOffset.latest());
		assertThat(ReadOffsetStrategy.Latest.getNext(offset, Optional.empty(), "42")).isEqualTo(ReadOffset.latest());
	}

	@Test // DATAREDIS-864
	public void nextMessageConsumerGroupShouldReturnLastSeenMessageId() {

		ReadOffset offset = ReadOffset.from("foo");

		assertThat(ReadOffsetStrategy.NextMessage.getFirst(offset, consumer)).isEqualTo(offset);
		assertThat(ReadOffsetStrategy.NextMessage.getNext(offset, consumer, "42")).isEqualTo(ReadOffset.from("42"));
	}

	@Test // DATAREDIS-864
	public void lastConsumedConsumerGroupShouldReturnLastSeenMessageId() {

		ReadOffset offset = ReadOffset.lastConsumed();

		assertThat(ReadOffsetStrategy.LastConsumed.getFirst(offset, consumer)).isEqualTo(ReadOffset.lastConsumed());
		assertThat(ReadOffsetStrategy.LastConsumed.getNext(offset, consumer, "42")).isEqualTo(ReadOffset.lastConsumed());
	}

	@Test // DATAREDIS-864
	public void latestConsumerGroupShouldReturnLatest() {

		ReadOffset offset = ReadOffset.latest();

		assertThat(ReadOffsetStrategy.Latest.getFirst(offset, consumer)).isEqualTo(ReadOffset.latest());
		assertThat(ReadOffsetStrategy.Latest.getNext(offset, consumer, "42")).isEqualTo(ReadOffset.latest());
	}

	@Test // DATAREDIS-864
	public void getStrategyShouldReturnAppropriateStrategy() {

		assertThat(ReadOffsetStrategy.getStrategy(ReadOffset.from("foo"))).isEqualTo(ReadOffsetStrategy.NextMessage);
		assertThat(ReadOffsetStrategy.getStrategy(ReadOffset.lastConsumed())).isEqualTo(ReadOffsetStrategy.LastConsumed);
		assertThat(ReadOffsetStrategy.getStrategy(ReadOffset.latest())).isEqualTo(ReadOffsetStrategy.Latest);
	}
}
