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
package org.springframework.data.redis.connection.jedis;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * @author Christoph Strobl
 */
public class JedisConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

	@Test // DATAREDIS-268
	public void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(JedisConverters.toListOfRedisClientInformation(""), equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	@Test // DATAREDIS-268
	public void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(JedisConverters.toListOfRedisClientInformation(null), equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	@Test // DATAREDIS-268
	public void convertingMultipleLiesToListOfRedisClientInfoReturnsListCorrectly() {

		StringBuilder sb = new StringBuilder();
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);
		sb.append("\r\n");
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);

		assertThat(JedisConverters.toListOfRedisClientInformation(sb.toString()).size(), equalTo(2));
	}

	@Test // DATAREDIS-330
	public void convertsSingleMapToRedisServerReturnsCollectionCorrectly() {

		Map<String, String> values = getRedisServerInfoMap("mymaster", 23697);
		List<RedisServer> servers = JedisConverters.toListOfRedisServer(Collections.singletonList(values));

		assertThat(servers.size(), is(1));
		verifyRedisServerInfo(servers.get(0), values);
	}

	@Test // DATAREDIS-330
	public void convertsMultipleMapsToRedisServerReturnsCollectionCorrectly() {

		List<Map<String, String>> vals = Arrays.asList(getRedisServerInfoMap("mymaster", 23697),
				getRedisServerInfoMap("yourmaster", 23680));
		List<RedisServer> servers = JedisConverters.toListOfRedisServer(vals);

		assertThat(servers.size(), is(vals.size()));
		for (int i = 0; i < vals.size(); i++) {
			verifyRedisServerInfo(servers.get(i), vals.get(i));
		}
	}

	@Test // DATAREDIS-330
	public void convertsRedisServersCorrectlyWhenGivenAnEmptyList() {
		assertThat(JedisConverters.toListOfRedisServer(Collections.<Map<String, String>> emptyList()), notNullValue());
	}

	@Test // DATAREDIS-330
	public void convertsRedisServersCorrectlyWhenGivenNull() {
		assertThat(JedisConverters.toListOfRedisServer(null), notNullValue());
	}

	/**
	 */
	@Test
	public void boundaryToBytesForZRangeByLexShouldReturnDefaultValueWhenBoundaryIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(null, defaultValue), is(defaultValue));
	}

	@Test // DATAREDIS-378
	public void boundaryToBytesForZRangeByLexShouldReturnDefaultValueWhenBoundaryValueIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.unbounded().getMax(), defaultValue),
				is(defaultValue));
	}

	@Test // DATAREDIS-378
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsIncluing() {

		assertThat(
				JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gte(JedisConverters.toBytes("a")).getMin(), null),
				is(JedisConverters.toBytes("[a")));
	}

	@Test // DATAREDIS-378
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsExcluding() {

		assertThat(
				JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt(JedisConverters.toBytes("a")).getMin(), null),
				is(JedisConverters.toBytes("(a")));
	}

	@Test // DATAREDIS-378
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsAString() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt("a").getMin(), null),
				is(JedisConverters.toBytes("(a")));
	}

	@Test // DATAREDIS-378
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsANumber() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt(1L).getMin(), null),
				is(JedisConverters.toBytes("(1")));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-378
	public void boundaryToBytesForZRangeByLexShouldThrowExceptionWhenBoundaryHoldsUnknownType() {
		JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt(new Date()).getMin(), null);
	}

	@Test // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldReturnDefaultValueWhenBoundaryIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRange(null, defaultValue), is(defaultValue));
	}

	@Test // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldReturnDefaultValueWhenBoundaryValueIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.unbounded().getMax(), defaultValue), is(defaultValue));
	}

	@Test // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsIncluing() {

		assertThat(
				JedisConverters.boundaryToBytesForZRange(Range.range().gte(JedisConverters.toBytes("a")).getMin(), null),
				is(JedisConverters.toBytes("a")));
	}

	@Test // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsExcluding() {

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.range().gt(JedisConverters.toBytes("a")).getMin(), null),
				is(JedisConverters.toBytes("(a")));
	}

	@Test // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsAString() {

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.range().gt("a").getMin(), null),
				is(JedisConverters.toBytes("(a")));
	}

	@Test // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldReturnValueCorrectlyWhenBoundaryIsANumber() {

		assertThat(JedisConverters.boundaryToBytesForZRange(Range.range().gt(1L).getMin(), null),
				is(JedisConverters.toBytes("(1")));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-352
	public void boundaryToBytesForZRangeByShouldThrowExceptionWhenBoundaryHoldsUnknownType() {
		JedisConverters.boundaryToBytesForZRange(Range.range().gt(new Date()).getMin(), null);
	}

	@Test // DATAREDIS-316
	public void toSetCommandExPxOptionShouldReturnEXforSeconds() {
		assertThat(JedisConverters.toSetCommandExPxArgument(Expiration.seconds(100)), equalTo(JedisConverters.toBytes("EX")));
	}

	@Test // DATAREDIS-316
	public void toSetCommandExPxOptionShouldReturnEXforMilliseconds() {

		assertThat(JedisConverters.toSetCommandExPxArgument(Expiration.milliseconds(100)),
				equalTo(JedisConverters.toBytes("PX")));
	}

	@Test // DATAREDIS-316
	public void toSetCommandExPxOptionShouldReturnEmptyArrayForNull() {
		assertThat(JedisConverters.toSetCommandExPxArgument(null), equalTo(new byte[] {}));
	}

	@Test // DATAREDIS-316
	public void toSetCommandNxXxOptionShouldReturnNXforAbsent() {
		assertThat(JedisConverters.toSetCommandNxXxArgument(SetOption.ifAbsent()), equalTo(JedisConverters.toBytes("NX")));
	}

	@Test // DATAREDIS-316
	public void toSetCommandNxXxOptionShouldReturnXXforAbsent() {
		assertThat(JedisConverters.toSetCommandNxXxArgument(SetOption.ifPresent()), equalTo(JedisConverters.toBytes("XX")));
	}

	@Test // DATAREDIS-316
	public void toSetCommandNxXxOptionShouldReturnEmptyArrayforUpsert() {
		assertThat(JedisConverters.toSetCommandNxXxArgument(SetOption.upsert()), equalTo(new byte[] {}));
	}

	private void verifyRedisServerInfo(RedisServer server, Map<String, String> values) {

		for (Map.Entry<String, String> entry : values.entrySet()) {
			assertThat(server.get(entry.getKey()), equalTo(entry.getValue()));
		}
	}

	private Map<String, String> getRedisServerInfoMap(String name, int port) {
		Map<String, String> map = new HashMap<>();
		map.put("name", name);
		map.put("ip", "127.0.0.1");
		map.put("port", Integer.toString(port));
		map.put("runid", "768c2926e5148d208713bf17cd5821e10f5388e2");
		map.put("flags", "master");
		map.put("pending-commands", "0");
		map.put("last-ping-sent", "0");
		map.put("last-ok-ping-reply", "534");
		map.put("last-ping-reply", "534");
		map.put("down-after-milliseconds", "30000");
		map.put("info-refresh", "147");
		map.put("role-reported", "master");
		map.put("role-reported-time", "41248339");
		map.put("config-epoch", "7");
		map.put("num-slaves", "2");
		map.put("num-other-sentinels", "2");
		map.put("quorum", "2");
		map.put("failover-timeout", "180000");
		map.put("parallel-syncs", "1");
		return map;
	}
}
