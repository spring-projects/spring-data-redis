/*
 * Copyright 2016 the original author or authors.
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
import static org.hamcrest.number.IsCloseTo.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveNumberCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void incrByDoubleShouldIncreaseValueCorrectly() {
		assertThat(connection.numberCommands().incrBy(KEY_1_BBUFFER, 1.5D).block(), is(closeTo(1.5D, 0D)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void incrByIntegerShouldIncreaseValueCorrectly() {
		assertThat(connection.numberCommands().incrBy(KEY_1_BBUFFER, 3).block(), is(3));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void decrByDoubleShouldDecreaseValueCorrectly() {
		assertThat(connection.numberCommands().decrBy(KEY_1_BBUFFER, 1.5D).block(), is(closeTo(-1.5D, 0D)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void decrByIntegerShouldDecreaseValueCorrectly() {
		assertThat(connection.numberCommands().decrBy(KEY_1_BBUFFER, 3).block(), is(-3));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hIncrByDoubleShouldIncreaseValueCorrectly() {

		nativeCommands.hset(KEY_1, KEY_1, "2");

		assertThat(connection.numberCommands().hIncrBy(KEY_1_BBUFFER, KEY_1_BBUFFER, 1.5D).block(), is(closeTo(3.5D, 0D)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void hIncrByIntegerShouldIncreaseValueCorrectly() {

		nativeCommands.hset(KEY_1, KEY_1, "2");

		assertThat(connection.numberCommands().hIncrBy(KEY_1_BBUFFER, KEY_1_BBUFFER, 3).block(), is(5));
	}
}
