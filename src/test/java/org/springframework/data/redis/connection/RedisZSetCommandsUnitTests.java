/*
 * Copyright 2023-present the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.test.util.IntRangeAssertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs;

/**
 * Unit tests for {@link RedisZSetCommands}.
 *
 * @author Mark Paluch
 * @author Moritz Halbritter
 */
class RedisZSetCommandsUnitTests {

	private static final byte[] KEY = "key".getBytes();

	private static final int OFFSET = 10;

	private static final int COUNT = 20;

	/**
	 * Runs the {@code long}-based default methods under test for real, while the {@link Limit}-based methods they
	 * delegate to remain stubbed.
	 */
	private final RedisZSetCommands commands = mock(RedisZSetCommands.class, CALLS_REAL_METHODS);

	private final ArgumentCaptor<Limit> limit = ArgumentCaptor.forClass(Limit.class);

	@Test // GH-2588
	void zAddArgsShouldReportEmpty() {

		assertThat(ZAddArgs.empty().isEmpty()).isTrue();
		assertThat(ZAddArgs.ifExists().isEmpty()).isFalse();
	}

	@Test // GH-3436
	void zRangeByScoreShouldPassOnLimitWithinIntegerRange() {

		this.commands.zRangeByScore(KEY, 1.0, 2.0, OFFSET, COUNT);

		verify(this.commands).zRangeByScore(eq(KEY), any(), this.limit.capture());
		assertCapturedLimit();
	}

	@Test // GH-3436
	void zRangeByScoreWithScoresShouldPassOnLimitWithinIntegerRange() {

		this.commands.zRangeByScoreWithScores(KEY, 1.0, 2.0, OFFSET, COUNT);

		verify(this.commands).zRangeByScoreWithScores(eq(KEY), any(), this.limit.capture());
		assertCapturedLimit();
	}

	@Test // GH-3436
	void zRevRangeByScoreShouldPassOnLimitWithinIntegerRange() {

		this.commands.zRevRangeByScore(KEY, 1.0, 2.0, OFFSET, COUNT);

		verify(this.commands).zRevRangeByScore(eq(KEY), any(), this.limit.capture());
		assertCapturedLimit();
	}

	@Test // GH-3436
	void zRevRangeByScoreWithScoresShouldPassOnLimitWithinIntegerRange() {

		this.commands.zRevRangeByScoreWithScores(KEY, 1.0, 2.0, OFFSET, COUNT);

		verify(this.commands).zRevRangeByScoreWithScores(eq(KEY), any(), this.limit.capture());
		assertCapturedLimit();
	}

	@Test // GH-3436
	void zRangeByScoreShouldRejectLimitOutsideIntegerRange() {
		assertRejectsOutOfRangeLimit("zRangeByScore",
				(offset, count) -> this.commands.zRangeByScore(KEY, 1.0, 2.0, offset, count));
	}

	@Test // GH-3436
	void zRangeByScoreWithScoresShouldRejectLimitOutsideIntegerRange() {
		assertRejectsOutOfRangeLimit("zRangeByScoreWithScores",
				(offset, count) -> this.commands.zRangeByScoreWithScores(KEY, 1.0, 2.0, offset, count));
	}

	@Test // GH-3436
	void zRevRangeByScoreShouldRejectLimitOutsideIntegerRange() {
		assertRejectsOutOfRangeLimit("zRevRangeByScore",
				(offset, count) -> this.commands.zRevRangeByScore(KEY, 1.0, 2.0, offset, count));
	}

	@Test // GH-3436
	void zRevRangeByScoreWithScoresShouldRejectLimitOutsideIntegerRange() {
		assertRejectsOutOfRangeLimit("zRevRangeByScoreWithScores",
				(offset, count) -> this.commands.zRevRangeByScoreWithScores(KEY, 1.0, 2.0, offset, count));
	}

	private void assertCapturedLimit() {

		assertThat(this.limit.getValue().getOffset()).isEqualTo(OFFSET);
		assertThat(this.limit.getValue().getCount()).isEqualTo(COUNT);
	}

	private static void assertRejectsOutOfRangeLimit(String command, RangeQuery query) {

		assertRejectsOutOfIntRange("Offset for " + command, (offset) -> query.run(offset, COUNT));
		assertRejectsOutOfIntRange("Count for " + command, (count) -> query.run(OFFSET, count));
	}

	/**
	 * Invokes one of the {@code long}-based range overloads under test.
	 */
	private interface RangeQuery {

		void run(long offset, long count);

	}

}
