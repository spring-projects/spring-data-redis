/*
 * Copyright 2014-2015 the original author or authors.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * @author Christoph Strobl
 */
public class JedisConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";
	private static final String CLUSTER_NODES_RESPONSE = "" //
			+ "ef570f86c7b1a953846668debc177a3a16733420 127.0.0.1:6379 myself,master - 0 0 1 connected 0-5460"
			+ System.getProperty("line.separator")
			+ "0f2ee5df45d18c50aca07228cc18b1da96fd5e84 127.0.0.1:6380 master - 0 1427718161587 2 connected 5461-10922"
			+ System.getProperty("line.separator")
			+ "3b9b8192a874fa8f1f09dbc0ee20afab5738eee7 127.0.0.1:6381 master - 0 1427718161587 3 connected 10923-16383"
			+ System.getProperty("line.separator")
			+ "8cad73f63eb996fedba89f041636f17d88cda075 127.0.0.1:7369 slave ef570f86c7b1a953846668debc177a3a16733420 0 1427718161587 1 connected";

	private static final String CLUSTER_NODE_WITH_SINGLE_SLOT_RESPONSE = "ef570f86c7b1a953846668debc177a3a16733420 127.0.0.1:6379 myself,master - 0 0 1 connected 3456";

	/**
	 * @see DATAREDIS-268
	 */
	@Test
	public void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(JedisConverters.toListOfRedisClientInformation(""), equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	/**
	 * @see DATAREDIS-268
	 */
	@Test
	public void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(JedisConverters.toListOfRedisClientInformation(null), equalTo(Collections.<RedisClientInfo> emptyList()));
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

		assertThat(JedisConverters.toListOfRedisClientInformation(sb.toString()).size(), equalTo(2));
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void convertsSingleMapToRedisServerReturnsCollectionCorrectly() {

		Map<String, String> values = getRedisServerInfoMap("mymaster", 23697);
		List<RedisServer> servers = JedisConverters.toListOfRedisServer(Collections.singletonList(values));

		assertThat(servers.size(), is(1));
		verifyRedisServerInfo(servers.get(0), values);
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void convertsMultipleMapsToRedisServerReturnsCollectionCorrectly() {

		List<Map<String, String>> vals = Arrays.asList(getRedisServerInfoMap("mymaster", 23697),
				getRedisServerInfoMap("yourmaster", 23680));
		List<RedisServer> servers = JedisConverters.toListOfRedisServer(vals);

		assertThat(servers.size(), is(vals.size()));
		for (int i = 0; i < vals.size(); i++) {
			verifyRedisServerInfo(servers.get(i), vals.get(i));
		}
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
	public void convertsRedisServersCorrectlyWhenGivenAnEmptyList() {
		assertThat(JedisConverters.toListOfRedisServer(Collections.<Map<String, String>> emptyList()), notNullValue());
	}

	/**
	 * @see DATAREDIS-330
	 */
	@Test
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

	/**
	 * @see DATAREDIS-378
	 */
	@Test
	public void boundaryToBytesForZRangeByLexShouldReturnDefaultValueWhenBoundaryValueIsNull() {

		byte[] defaultValue = "tyrion".getBytes();

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.unbounded().getMax(), defaultValue),
				is(defaultValue));
	}

	/**
	 * @see DATAREDIS-378
	 */
	@Test
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsIncluing() {

		assertThat(
				JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gte(JedisConverters.toBytes("a")).getMin(), null),
				is(JedisConverters.toBytes("[a")));
	}

	/**
	 * @see DATAREDIS-378
	 */
	@Test
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsExcluding() {

		assertThat(
				JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt(JedisConverters.toBytes("a")).getMin(), null),
				is(JedisConverters.toBytes("(a")));
	}

	/**
	 * @see DATAREDIS-378
	 */
	@Test
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsAString() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt("a").getMin(), null),
				is(JedisConverters.toBytes("(a")));
	}

	/**
	 * @see DATAREDIS-378
	 */
	@Test
	public void boundaryToBytesForZRangeByLexShouldReturnValueCorrectlyWhenBoundaryIsANumber() {

		assertThat(JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt(1L).getMin(), null),
				is(JedisConverters.toBytes("(1")));
	}

	/**
	 * @see DATAREDIS-378
	 */
	@Test(expected = IllegalArgumentException.class)
	public void boundaryToBytesForZRangeByLexShouldThrowExceptionWhenBoundaryHoldsUnknownType() {
		JedisConverters.boundaryToBytesForZRangeByLex(Range.range().gt(new Date()).getMin(), null);
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void toSetOfRedisClusterNodesShouldConvertSingleStringNodesResponseCorrectly() {

		Iterator<RedisClusterNode> nodes = JedisConverters.toSetOfRedisClusterNodes(CLUSTER_NODES_RESPONSE).iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6379));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().getLowerBound(), is(0));
		assertThat(node.getSlotRange().getUpperBound(), is(5460));

		node = nodes.next(); // 127.0.0.1:6380
		assertThat(node.getId(), is("0f2ee5df45d18c50aca07228cc18b1da96fd5e84"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6380));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().getLowerBound(), is(5461));
		assertThat(node.getSlotRange().getUpperBound(), is(10922));

		node = nodes.next(); // 127.0.0.1:638
		assertThat(node.getId(), is("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6381));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().getLowerBound(), is(10923));
		assertThat(node.getSlotRange().getUpperBound(), is(16383));

		node = nodes.next(); // 127.0.0.1:7369
		assertThat(node.getId(), is("8cad73f63eb996fedba89f041636f17d88cda075"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(7369));
		assertThat(node.getType(), is(NodeType.SLAVE));
		assertThat(node.getMasterId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getSlotRange(), nullValue());
	}

	/**
	 * @see DATAREDIS-315
	 */
	@Test
	public void toSetOfRedisClusterNodesShouldConvertNodesWihtSingleSlotCorrectly() {

		Iterator<RedisClusterNode> nodes = JedisConverters.toSetOfRedisClusterNodes(CLUSTER_NODE_WITH_SINGLE_SLOT_RESPONSE)
				.iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6379));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().getLowerBound(), is(3456));
		assertThat(node.getSlotRange().getUpperBound(), is(3456));
	}

	private void verifyRedisServerInfo(RedisServer server, Map<String, String> values) {

		for (Map.Entry<String, String> entry : values.entrySet()) {
			assertThat(server.get(entry.getKey()), equalTo(entry.getValue()));
		}
	}

	private Map<String, String> getRedisServerInfoMap(String name, int port) {
		Map<String, String> map = new HashMap<String, String>();
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
