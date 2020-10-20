/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;

import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveNumberCommandsIntegrationTests extends LettuceReactiveCommandsTestSupport {

	public LettuceReactiveNumberCommandsIntegrationTests(Fixture fixture) {
		super(fixture);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void incrByDoubleShouldIncreaseValueCorrectly() {
		assertThat(connection.numberCommands().incrBy(KEY_1_BBUFFER, 1.5D).block()).isCloseTo(1.5D, offset(0D));
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void incrByIntegerShouldIncreaseValueCorrectly() {
		assertThat(connection.numberCommands().incrBy(KEY_1_BBUFFER, 3).block()).isEqualTo(3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void decrByDoubleShouldDecreaseValueCorrectly() {
		assertThat(connection.numberCommands().decrBy(KEY_1_BBUFFER, 1.5D).block()).isCloseTo(-1.5D, offset(0D));
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void decrByIntegerShouldDecreaseValueCorrectly() {
		assertThat(connection.numberCommands().decrBy(KEY_1_BBUFFER, 3).block()).isEqualTo(-3);
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hIncrByDoubleShouldIncreaseValueCorrectly() {

		nativeCommands.hset(KEY_1, KEY_1, "2");

		assertThat(connection.numberCommands().hIncrBy(KEY_1_BBUFFER, KEY_1_BBUFFER, 1.5D).block()).isCloseTo(3.5D,
				offset(0D));
	}

	@ParameterizedRedisTest // DATAREDIS-525
	void hIncrByIntegerShouldIncreaseValueCorrectly() {

		nativeCommands.hset(KEY_1, KEY_1, "2");

		assertThat(connection.numberCommands().hIncrBy(KEY_1_BBUFFER, KEY_1_BBUFFER, 3).block()).isEqualTo(5);
	}
}
