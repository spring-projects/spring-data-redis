/*
 * Copyright 2025-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.core.ExpireChanges.ExpiryChangeState;

/**
 * Unit tests for {@link ExpireChanges}.
 *
 * @author Moritz Halbritter
 */
class ExpireChangesUnitTests {

	@Test // GH-3436
	void largeStateValuesShouldNotBeMisclassified() {

		ExpireChanges<String> changes = ExpireChanges.of(List.of("key"), List.of(4294967294L));

		assertThat(changes.stateOf("key")).isEqualTo(new ExpiryChangeState(4294967294L));
		assertThat(changes.stateOf("key")).isNotEqualTo(ExpiryChangeState.DOES_NOT_EXIST);
	}
}
