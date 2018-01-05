/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * @author Christoph Strobl
 */
public class DefaultTypedTupleUnitTests {

	private static final TypedTuple<String> WITH_SCORE_1 = new DefaultTypedTuple<>("foo", 1D);
	private static final TypedTuple<String> ANOTHER_ONE_WITH_SCORE_1 = new DefaultTypedTuple<>("another", 1D);
	private static final TypedTuple<String> WITH_SCORE_2 = new DefaultTypedTuple<>("bar", 2D);
	private static final TypedTuple<String> WITH_SCORE_NULL = new DefaultTypedTuple<>("foo", null);

	@Test // DATAREDIS-294
	public void compareToShouldUseScore() {

		assertThat(WITH_SCORE_1.compareTo(WITH_SCORE_2), equalTo(-1));
		assertThat(WITH_SCORE_2.compareTo(WITH_SCORE_1), equalTo(1));
		assertThat(WITH_SCORE_1.compareTo(ANOTHER_ONE_WITH_SCORE_1), equalTo(0));
	}

	@Test // DATAREDIS-294
	public void compareToShouldConsiderGivenNullAsZeroScore() {

		assertThat(WITH_SCORE_1.compareTo(null), equalTo(1));
		assertThat(WITH_SCORE_NULL.compareTo(null), equalTo(0));
	}

	@Test // DATAREDIS-294
	public void compareToShouldConsiderNullScoreAsZeroScore() {

		assertThat(WITH_SCORE_1.compareTo(WITH_SCORE_NULL), equalTo(1));
		assertThat(WITH_SCORE_NULL.compareTo(WITH_SCORE_1), equalTo(-1));
	}
}
