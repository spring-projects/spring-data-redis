/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.test.util;

import static org.assertj.core.api.Assertions.*;

import java.util.function.LongConsumer;

/**
 * Assertions for {@code long} values that have to be narrowed to {@code int} before they reach a driver.
 *
 * @author Moritz Halbritter
 */
public final class IntRangeAssertions {

	/**
	 * Smallest {@code long} that no longer fits into an {@code int}.
	 */
	public static final long ABOVE_INT_RANGE = (long) Integer.MAX_VALUE + 1L;

	/**
	 * Largest negative {@code long} that no longer fits into an {@code int}.
	 */
	public static final long BELOW_INT_RANGE = (long) Integer.MIN_VALUE - 1L;

	private IntRangeAssertions() {
	}

	/**
	 * Assert that {@code conversion} rejects values on both sides of the {@code int} range. The message is only matched
	 * for the upper bound: some option builders reject negative values up-front with a message of their own, so the lower
	 * bound never reaches the conversion.
	 *
	 * @param subject the expected message fragment, e.g. {@code "Count for scan in Jedis"}.
	 * @param conversion invokes the code under test with the given value.
	 */
	public static void assertRejectsOutOfIntRange(String subject, LongConsumer conversion) {

		assertThatIllegalArgumentException().isThrownBy(() -> conversion.accept(ABOVE_INT_RANGE))
				.withMessageContaining(subject);
		assertThatIllegalArgumentException().isThrownBy(() -> conversion.accept(BELOW_INT_RANGE));
	}

}
