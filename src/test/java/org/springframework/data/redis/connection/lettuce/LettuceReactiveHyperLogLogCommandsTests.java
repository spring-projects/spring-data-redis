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
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

import java.util.Arrays;

import org.junit.Test;
import org.springframework.data.redis.test.util.LettuceRedisClientProvider;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveHyperLogLogCommandsTests extends LettuceReactiveCommandsTestsBase {

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pfAddShouldAddToNonExistingKeyCorrectly() {

		assertThat(connection.hyperLogLogCommands()
				.pfAdd(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block(), is(1L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pfAddShouldReturnZeroWhenValueAlreadyExists() {

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(KEY_1, new String[] { VALUE_3 });

		assertThat(connection.hyperLogLogCommands().pfAdd(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER)).block(), is(0L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pfCountShouldReturnCorrectly() {

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });

		assertThat(connection.hyperLogLogCommands().pfCount(KEY_1_BBUFFER).block(), is(2L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pfCountWithMultipleKeysShouldReturnCorrectly() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(connection.hyperLogLogCommands().pfCount(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block(), is(3L));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void pfMergeShouldWorkCorrectly() {

		assumeThat(clientProvider instanceof LettuceRedisClientProvider, is(true));

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(
				connection.hyperLogLogCommands().pfMerge(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block(),
				is(true));

		assertThat(nativeCommands.pfcount(new String[] { KEY_3 }), is(3L));
	}

}
