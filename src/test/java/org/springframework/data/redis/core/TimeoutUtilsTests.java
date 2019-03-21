/*
 * Copyright 2013 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Unit test of {@link TimeoutUtils}
 * 
 * @author Jennifer Hickey
 */
public class TimeoutUtilsTests {

	@Test
	public void testConvertMoreThanOneSecond() {
		assertThat(TimeoutUtils.toSeconds(2010, TimeUnit.MILLISECONDS)).isEqualTo(2);
	}

	@Test
	public void testConvertLessThanOneSecond() {
		assertThat(TimeoutUtils.toSeconds(999, TimeUnit.NANOSECONDS)).isEqualTo(1);
	}

	@Test
	public void testConvertZeroSeconds() {
		assertThat(TimeoutUtils.toSeconds(0, TimeUnit.MINUTES)).isEqualTo(0);
	}

	@Test
	public void testConvertNegativeSecondsGreaterThanNegativeOne() {
		// Ensure we convert this to 0 as before, though ideally we wouldn't accept negative values
		assertThat(TimeoutUtils.toSeconds(-123, TimeUnit.MILLISECONDS)).isEqualTo(0);
	}

	@Test
	public void testConvertNegativeSecondsEqualNegativeOne() {
		assertThat(TimeoutUtils.toSeconds(-1111, TimeUnit.MILLISECONDS)).isEqualTo(-1);
	}

	@Test
	public void testConvertNegativeSecondsLessThanNegativeOne() {
		assertThat(TimeoutUtils.toSeconds(-2344, TimeUnit.MILLISECONDS)).isEqualTo(-2);
	}

	@Test
	public void testConvertMoreThanOneMilli() {
		assertThat(TimeoutUtils.toMillis(2010, TimeUnit.MICROSECONDS)).isEqualTo(2);
	}

	@Test
	public void testConvertLessThanOneMilli() {
		assertThat(TimeoutUtils.toMillis(999, TimeUnit.NANOSECONDS)).isEqualTo(1);
	}

	@Test
	public void testConvertZeroMillis() {
		assertThat(TimeoutUtils.toMillis(0, TimeUnit.SECONDS)).isEqualTo(0);
	}

	@Test
	public void testConvertNegativeMillisGreaterThanNegativeOne() {
		// Ensure we convert this to 0 as before, though ideally we wouldn't accept negative values
		assertThat(TimeoutUtils.toMillis(-123, TimeUnit.MICROSECONDS)).isEqualTo(0);
	}

	@Test
	public void testConvertNegativeMillisEqualNegativeOne() {
		assertThat(TimeoutUtils.toMillis(-1111, TimeUnit.MICROSECONDS)).isEqualTo(-1);
	}

	@Test
	public void testConvertNegativeMillisLessThanNegativeOne() {
		assertThat(TimeoutUtils.toMillis(-2344, TimeUnit.MICROSECONDS)).isEqualTo(-2);
	}
}
