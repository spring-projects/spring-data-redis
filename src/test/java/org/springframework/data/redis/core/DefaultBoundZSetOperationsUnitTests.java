/*
 * Copyright 2021-2023 the original author or authors.
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

import static org.mockito.ArgumentMatchers.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;

/**
 * Unit tests for {@link DefaultBoundZSetOperations}.
 *
 * @author Christoph Strobl
 */
class DefaultBoundZSetOperationsUnitTests {

	DefaultBoundZSetOperations zSetOperations;
	ConnectionMockingRedisTemplate template;

	@BeforeEach
	void beforeEach() {

		template = ConnectionMockingRedisTemplate.template();
		zSetOperations = new DefaultBoundZSetOperations<>("key", template);
	}

	@Test // GH-1816
	void delegatesRemoveRangeByLex() {

		Range range = Range.range().gte("alpha").lte("omega");
		zSetOperations.removeRangeByLex(range);

		template.verify().zRemRangeByLex(eq(template.serializeKey("key")), eq(range));
	}
}
