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
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

import java.util.Arrays;

import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveClusterHyperLogLogCommandsTests extends LettuceReactiveClusterCommandsTestsBase {

	@Test // DATAREDIS-525
	public void pfCountWithMultipleKeysShouldReturnCorrectlyWhenKeysMapToSameSlot() {

		nativeCommands.pfadd(SAME_SLOT_KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(SAME_SLOT_KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(connection.hyperLogLogCommands().pfCount(Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER))
				.block()).isEqualTo(3L);
	}

	@Test // DATAREDIS-525
	public void pfMergeShouldWorkCorrectlyWhenKeysMapToSameSlot() {

		nativeCommands.pfadd(SAME_SLOT_KEY_1, new String[] { VALUE_1, VALUE_2 });
		nativeCommands.pfadd(SAME_SLOT_KEY_2, new String[] { VALUE_2, VALUE_3 });

		assertThat(connection.hyperLogLogCommands()
				.pfMerge(SAME_SLOT_KEY_3_BBUFFER, Arrays.asList(SAME_SLOT_KEY_1_BBUFFER, SAME_SLOT_KEY_2_BBUFFER)).block())
						.isTrue();

		assertThat(nativeCommands.pfcount(new String[] { SAME_SLOT_KEY_3 })).isEqualTo(3L);
	}

}
