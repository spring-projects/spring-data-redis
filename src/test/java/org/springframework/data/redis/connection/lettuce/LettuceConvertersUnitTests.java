/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * @author Christoph Strobl
 */
public class LettuceConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

	/**
	 * @see DATAREDIS-268
	 */
	@Test
	public void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(""), equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	/**
	 * @see DATAREDIS-268
	 */
	@Test
	public void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(null),
				equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	/**
	 * @see DATAREDIS-268
	 */
	@Test
	public void convertingMultipleLiesToListOfRedisClientInfoReturnsListCorrectly() {

		StringBuilder sb = new StringBuilder();
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);
		sb.append("\r\n");
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);

		assertThat(LettuceConverters.toListOfRedisClientInformation(sb.toString()).size(), equalTo(2));
	}

}
