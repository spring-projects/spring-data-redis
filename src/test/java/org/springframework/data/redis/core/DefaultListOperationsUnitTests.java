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
package org.springframework.data.redis.core;

import static org.springframework.data.redis.test.util.IntRangeAssertions.*;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultListOperations}.
 *
 * @author Moritz Halbritter
 */
class DefaultListOperationsUnitTests {

	private final ListOperations<String, String> operations = ConnectionMockingRedisTemplate.<String, String> template()
			.opsForList();

	@Test // GH-3436
	void leftPopShouldRejectTimeoutOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Timeout for leftPop",
				(timeout) -> this.operations.leftPop("key", timeout, TimeUnit.SECONDS));
	}

	@Test // GH-3436
	void rightPopShouldRejectTimeoutOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Timeout for rightPop",
				(timeout) -> this.operations.rightPop("key", timeout, TimeUnit.SECONDS));
	}

	@Test // GH-3436
	void rightPopAndLeftPushShouldRejectTimeoutOutsideIntegerRange() {
		assertRejectsOutOfIntRange("Timeout for rightPopAndLeftPush",
				(timeout) -> this.operations.rightPopAndLeftPush("src", "dst", timeout, TimeUnit.SECONDS));
	}

}
