/*
 * Copyright 2013-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


/**
 * Unit test of {@link TimeoutUtils}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Chris Bono
 */
class TimeoutUtilsUnitTests {

	@Test
	void testConvertMoreThanOneSecond() {
		assertThat(TimeoutUtils.toSeconds(2010, TimeUnit.MILLISECONDS)).isEqualTo(2);
	}

	@Test
	void testConvertLessThanOneSecond() {
		assertThat(TimeoutUtils.toSeconds(999, TimeUnit.NANOSECONDS)).isOne();
	}

	@Test
	void testConvertZeroSeconds() {
		assertThat(TimeoutUtils.toSeconds(0, TimeUnit.MINUTES)).isZero();
	}

	@Test
	void testConvertNegativeSecondsGreaterThanNegativeOne() {
		// Ensure we convert this to 0 as before, though ideally we wouldn't accept negative values
		assertThat(TimeoutUtils.toSeconds(-123, TimeUnit.MILLISECONDS)).isZero();
	}

	@Test
	void testConvertNegativeSecondsEqualNegativeOne() {
		assertThat(TimeoutUtils.toSeconds(-1111, TimeUnit.MILLISECONDS)).isEqualTo(-1);
	}

	@Test
	void testConvertNegativeSecondsLessThanNegativeOne() {
		assertThat(TimeoutUtils.toSeconds(-2344, TimeUnit.MILLISECONDS)).isEqualTo(-2);
	}

	@Test
	void testConvertMoreThanOneMilli() {
		assertThat(TimeoutUtils.toMillis(2010, TimeUnit.MICROSECONDS)).isEqualTo(2);
	}

	@Test
	void testConvertLessThanOneMilli() {
		assertThat(TimeoutUtils.toMillis(999, TimeUnit.NANOSECONDS)).isOne();
	}

	@Test
	void testConvertZeroMillis() {
		assertThat(TimeoutUtils.toMillis(0, TimeUnit.SECONDS)).isZero();
	}

	@Test
	void testConvertNegativeMillisGreaterThanNegativeOne() {
		// Ensure we convert this to 0 as before, though ideally we wouldn't accept negative values
		assertThat(TimeoutUtils.toMillis(-123, TimeUnit.MICROSECONDS)).isZero();
	}

	@Test
	void testConvertNegativeMillisEqualNegativeOne() {
		assertThat(TimeoutUtils.toMillis(-1111, TimeUnit.MICROSECONDS)).isEqualTo(-1);
	}

	@Test
	void testConvertNegativeMillisLessThanNegativeOne() {
		assertThat(TimeoutUtils.toMillis(-2344, TimeUnit.MICROSECONDS)).isEqualTo(-2);
	}

	@Test // DATAREDIS-815
	void hasMillisReturnsFalseForTimeoutOfExactSeconds() {
		assertThat(TimeoutUtils.hasMillis(Duration.ofSeconds(1))).isFalse();
	}

	@Test // DATAREDIS-815
	void hasMillisReturnsTrueForTimeoutWithMsec() {
		assertThat(TimeoutUtils.hasMillis(Duration.ofMillis(1500))).isTrue();
	}

	@Test // DATAREDIS-815
	void hasMillisReturnsTrueForTimeoutLessThanOneSecond() {
		assertThat(TimeoutUtils.hasMillis(Duration.ofMillis(500))).isTrue();
	}

	@ParameterizedTest // GH-2975
	@DisplayName("isZeroOrGreaterOneSecReturnsTrueFor")
	@MethodSource
	void isZeroOrGreaterOneSecReturnsTrueFor(Duration timeout) {
		assertThat(TimeoutUtils.isZeroOrGreaterThanOneSecond(timeout)).isTrue();
	}

	static Stream<Arguments> isZeroOrGreaterOneSecReturnsTrueFor() {
		return Stream.of(
				argumentSet("zero", Duration.ZERO),
				argumentSet("oneSecond", Duration.ofSeconds(1)),
				argumentSet("greaterThanOneSecond", Duration.ofMillis(1001)),
				argumentSet("greaterThanOneSecondIncludingNano", Duration.ofSeconds(1).plusNanos(500_000_000))
		);
	}

	@Test // GH-2975
	void isZeroOrGreaterOneSecReturnsFalseForLessThanOneSec() {
		assertThat(TimeoutUtils.isZeroOrGreaterThanOneSecond(Duration.ofMillis(500))).isFalse();
	}
}
