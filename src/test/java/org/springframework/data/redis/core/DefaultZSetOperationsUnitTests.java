/*
 * Copyright 2021-2022 the original author or authors.
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

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Unit tests for {@link DefaultZSetOperations}.
 *
 * @author Christoph Strobl
 */
class DefaultZSetOperationsUnitTests {

	DefaultZSetOperations zSetOperations;
	ConnectionMockingRedisTemplate template;

	@BeforeEach
	void beforeEach() {

		template = ConnectionMockingRedisTemplate.template();
		zSetOperations = new DefaultZSetOperations<>(template);
	}

	@Test // GH-1816
	void delegatesRemoveRangeByLex() {

		Range<String> range = Range.closed("alpha", "omega");
		zSetOperations.removeRangeByLex("key", range);

		template.verify().zRemRangeByLex(eq(template.serializeKey("key")), any(Range.class));
	}

	@Test // GH-1794
	void delegatesAddIfAbsent() {

		zSetOperations.addIfAbsent("key", "value", 1D);

		template.verify().zAdd(eq(template.serializeKey("key")), eq(1D), eq(template.serializeKey("value")),
				eq(ZAddArgs.ifNotExists()));
	}

	@Test // GH-1794
	void delegatesAddIfAbsentForTuples() {

		zSetOperations.addIfAbsent("key", Collections.singleton(TypedTuple.of("value", 1D)));

		template.verify().zAdd(eq(template.serializeKey("key")), any(Set.class), eq(ZAddArgs.ifNotExists()));
	}
}
