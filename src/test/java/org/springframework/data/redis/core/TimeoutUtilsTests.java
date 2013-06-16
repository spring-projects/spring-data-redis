/*
 * Copyright 2013 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test of {@link TimeoutUtils}
 * 
 * @author Jennifer Hickey
 * 
 */
public class TimeoutUtilsTests {

	@Test
	public void testConvertMoreThanOneSecond() {
		assertEquals(2, TimeoutUtils.toSeconds(2010, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testConvertLessThanOneSecond() {
		assertEquals(1, TimeoutUtils.toSeconds(999, TimeUnit.NANOSECONDS));
	}

	@Test
	public void testConvertZeroSeconds() {
		assertEquals(0, TimeoutUtils.toSeconds(0, TimeUnit.MINUTES));
	}
	
	@Test
	public void testConvertNegativeSecondsGreaterThanNegativeOne() {
		// Ensure we convert this to 0 as before, though ideally we wouldn't accept negative values
		assertEquals(0,TimeoutUtils.toSeconds(-123, TimeUnit.MILLISECONDS));
	}
	
	@Test
	public void testConvertNegativeSecondsEqualNegativeOne() {
		assertEquals(-1,TimeoutUtils.toSeconds(-1111, TimeUnit.MILLISECONDS));
	}
	
	@Test
	public void testConvertNegativeSecondsLessThanNegativeOne() {
		assertEquals(-2,TimeoutUtils.toSeconds(-2344, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testConvertMoreThanOneMilli() {
		assertEquals(2, TimeoutUtils.toMillis(2010, TimeUnit.MICROSECONDS));
	}

	@Test
	public void testConvertLessThanOneMilli() {
		assertEquals(1, TimeoutUtils.toMillis(999, TimeUnit.NANOSECONDS));
	}

	@Test
	public void testConvertZeroMillis() {
		assertEquals(0, TimeoutUtils.toMillis(0, TimeUnit.SECONDS));
	}

	@Test
	public void testConvertNegativeMillisGreaterThanNegativeOne() {
		// Ensure we convert this to 0 as before, though ideally we wouldn't accept negative values
		assertEquals(0,TimeoutUtils.toMillis(-123, TimeUnit.MICROSECONDS));
	}

	@Test
	public void testConvertNegativeMillisEqualNegativeOne() {
		assertEquals(-1,TimeoutUtils.toMillis(-1111, TimeUnit.MICROSECONDS));
	}

	@Test
	public void testConvertNegativeMillisLessThanNegativeOne() {
		assertEquals(-2,TimeoutUtils.toMillis(-2344, TimeUnit.MICROSECONDS));
	}
}
