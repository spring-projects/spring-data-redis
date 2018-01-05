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
package org.springframework.data.redis.core.types;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.types.RedisClientInfo.RedisClientInfoBuilder;

/**
 * @author Christoph Strobl
 */
public class RedisClientInfoUnitTests {

	private final String SOURCE_WITH_PLACEHOLDER = "addr=127.0.0.1:57013#fd=6#name=client-1#age=16#idle=0#flags=N#db=0#sub=0#psub=0#multi=-1#qbuf=0#qbuf-free=32768#obl=0#oll=0#omem=0#events=r#cmd=client";
	private final String SINGLE_LINE = SOURCE_WITH_PLACEHOLDER.replace('#', ' ');
	private final String[] VALUES = SOURCE_WITH_PLACEHOLDER.split("#");

	private RedisClientInfo info;

	@Before
	public void setUp() {
		info = RedisClientInfoBuilder.fromString(SINGLE_LINE);
	}

	@Test
	public void testBuilderShouldReadsInfoCorrectlyFromSingleLineString() {
		assertValues(info, VALUES);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetRequiresNonNullKey() {
		info.get((String) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetRequiresNonBlankKey() {
		info.get("");
	}

	@Test
	public void testGetReturnsNullForPropertiesNotAvailable() {
		Assert.assertThat(info.get("foo-bar"), IsEqual.equalTo(null));
	}

	private void assertValues(RedisClientInfo info, String[] values) {
		for (String potentialValue : values) {
			if (potentialValue.contains("=")) {
				String[] keyValuePair = potentialValue.split("=");
				Assert.assertThat(info.get(keyValuePair[0]), Is.is(keyValuePair[1]));
			} else {
				Assert.assertThat(info.get(potentialValue), IsNot.not(null));
			}
		}

	}

}
