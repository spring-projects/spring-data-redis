/*
 * Copyright 2016. the original author or authors.
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

package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisStringCommands;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveClusterStringCommandsTests extends LettuceReactiveClusterCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mSetNXShouldAddMultipleKeyValueParisWhenMappedToSameSlot() {

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(SAME_SLOT_KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(SAME_SLOT_KEY_2_BBUFFER, VALUE_2_BBUFFER);

		connection.stringCommands().mSetNX(map).block();

		assertThat(nativeCommands.get(SAME_SLOT_KEY_1), is(equalTo(VALUE_1)));
		assertThat(nativeCommands.get(SAME_SLOT_KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mSetNXShouldNotAddMultipleKeyValueParisWhenAlreadyExitAndMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<>();
		map.put(SAME_SLOT_KEY_1_BBUFFER, VALUE_1_BBUFFER);
		map.put(SAME_SLOT_KEY_2_BBUFFER, VALUE_2_BBUFFER);

		assertThat(connection.stringCommands().mSetNX(map).block(), is(false));

		assertThat(nativeCommands.exists(SAME_SLOT_KEY_1), is(false));
		assertThat(nativeCommands.get(SAME_SLOT_KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bitOpAndShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.AND, SAME_SLOT_KEY_3_BBUFFER).block(), is(7L));
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3), is(equalTo("value-0")));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void bitOpOrShouldWorkAsExpectedWhenKeysMapToSameSlot() {

		nativeCommands.set(SAME_SLOT_KEY_1, VALUE_1);
		nativeCommands.set(SAME_SLOT_KEY_2, VALUE_2);

		assertThat(connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.OR, SAME_SLOT_KEY_3_BBUFFER).block(), is(7L));
		assertThat(nativeCommands.get(SAME_SLOT_KEY_3), is(equalTo(VALUE_3)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test(expected = IllegalArgumentException.class)
	public void bitNotShouldThrowExceptionWhenMoreThanOnSourceKeyAndKeysMapToSameSlot() {

		connection.stringCommands().bitOp(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER),
				RedisStringCommands.BitOperation.NOT, SAME_SLOT_KEY_3_BBUFFER).block();
	}

}
