/*
 * Copyright 2016-present the original author or authors.
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
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestSupport.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.test.condition.EnabledOnRedisVersion;

/**
 * @author Christoph Strobl
 * @author Viktoriya Kutsarova
 * @since 2.0
 */
class LettuceReactiveClusterStringCommandsIntegrationTests extends LettuceReactiveClusterTestSupport {

	@Test // DATAREDIS-525
	void mSetNXShouldAddMultipleKeyValuePairsWhenMappedToSameSlot() {

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(SAME_SLOT_KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(SAME_SLOT_KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSetNX(map).block();

		assertThat(nativeCommands.get(SAME_SLOT_KEY_1)).isEqualTo(VALUE_1);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-525
	void mSetNXShouldNotAddMultipleKeyValuePairsWhenAlreadyExitAndMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(SAME_SLOT_KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(SAME_SLOT_KEY_2_BBUFFER, VALUE_2_BBUFFER);

		assertThat(connection.stringCommands().mSetNX(map).block()).isFalse();

		assertThat(nativeCommands.exists(SAME_SLOT_KEY_1)).isEqualTo(0L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_2)).isEqualTo(VALUE_2);
	}

	@Test // DATAREDIS-525
	void bitOpAndShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.AND, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(7L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isEqualTo("value-0");
	}

	@Test // DATAREDIS-525
	void bitOpOrShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.OR, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(7L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isEqualTo(VALUE_3);
	}

	@Test // GH-3250
	void bitOpXorShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.XOR, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(7L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isNotNull();
	}

	@Test // GH-3250
	void bitOpNotShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER),
				RedisStringCommands.BitOperation.NOT, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(7L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isNotNull();
	}

	@Test // DATAREDIS-525
	void bitNotShouldThrowExceptionWhenMoreThanOnSourceKeyAndKeysMapToSameSlot() {
		assertThatIllegalArgumentException().isThrownBy(
				() -> connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
						RedisStringCommands.BitOperation.NOT, SAME_SLOT_KEY_3_BBUFFER).block());
	}

	@Test // GH-3250
	@EnabledOnRedisVersion("8.2")
	void bitOpDiffShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, "foobar");
		nativeCommands.set(SAME_SLOT_KEY_2, "abcdef");

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.DIFF, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(6L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isNotNull();
	}

	@Test // GH-3250
	@EnabledOnRedisVersion("8.2")
	void bitOpDiff1ShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, "foobar");
		nativeCommands.set(SAME_SLOT_KEY_2, "abcdef");

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.DIFF1, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(6L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isNotNull();
	}

	@Test // GH-3250
	@EnabledOnRedisVersion("8.2")
	void bitOpAndorShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.ANDOR, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(7L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isNotNull();
	}

	@Test // GH-3250
	@EnabledOnRedisVersion("8.2")
	void bitOpOneShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.ONE, SAME_SLOT_KEY_3_BBUFFER).block()).isEqualTo(7L);
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3)).isNotNull();
	}

}
