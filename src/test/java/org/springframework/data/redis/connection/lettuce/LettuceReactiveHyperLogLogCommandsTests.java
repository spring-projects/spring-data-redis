/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.junit.Assume.*;

import java.util.Arrays;

import org.junit.Test;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceReactiveHyperLogLogCommandsTests extends LettuceReactiveCommandsTestsBase {

	@Test // DATAREDIS-525
	public void pfAddShouldAddToNonExistingKeyCorrectly() {

		assertThat(connection.hyperLogLogCommands()
				.pfAdd(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER, VALUE_2_BBUFFER, VALUE_3_BBUFFER)).block()).isEqualTo(1L);
	}

	@Test // DATAREDIS-525
	public void pfAddShouldReturnZeroWhenValueAlreadyExists() {

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(KEY_1, new String[] { VALUE_3 });

		assertThat(connection.hyperLogLogCommands().pfAdd(KEY_1_BBUFFER, Arrays.asList(VALUE_1_BBUFFER)).block())
				.isEqualTo(0L);
	}

	@Test // DATAREDIS-525
	public void pfCountShouldReturnCorrectly() {

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });

		assertThat(connection.hyperLogLogCommands().pfCount(KEY_1_BBUFFER).block()).isEqualTo(2L);
	}

	@Test // DATAREDIS-525
	public void pfCountWithMultipleKeysShouldReturnCorrectly() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(connection.hyperLogLogCommands().pfCount(Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block())
				.isEqualTo(3L);
	}

	@Test // DATAREDIS-525
	public void pfMergeShouldWorkCorrectly() {

		assumeTrue(connectionProvider instanceof StandaloneConnectionProvider);

		nativeCommands.pfadd(KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(
				connection.hyperLogLogCommands().pfMerge(KEY_3_BBUFFER, Arrays.asList(KEY_1_BBUFFER, KEY_2_BBUFFER)).block())
						.isTrue();

		assertThat(nativeCommands.pfcount(new String[] { KEY_3 })).isEqualTo(3L);
	}
}
