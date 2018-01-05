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
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * @author Christoph Strobl
 */
public class LettuceConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

	@Test // DATAREDIS-268
	public void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(""),
				equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	@Test // DATAREDIS-268
	public void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(null),
				equalTo(Collections.<RedisClientInfo> emptyList()));
	}

	@Test // DATAREDIS-268
	public void convertingMultipleLiesToListOfRedisClientInfoReturnsListCorrectly() {

		StringBuilder sb = new StringBuilder();
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);
		sb.append("\r\n");
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);

		assertThat(LettuceConverters.toListOfRedisClientInformation(sb.toString()).size(), equalTo(2));
	}

	@Test // DATAREDIS-315
	public void partitionsToClusterNodesShouldReturnEmptyCollectionWhenPartionsDoesNotContainElements() {
		assertThat(LettuceConverters.partitionsToClusterNodes(new Partitions()), notNullValue());
	}

	@Test // DATAREDIS-315
	public void partitionsToClusterNodesShouldConvertPartitionCorrctly() {

		Partitions partitions = new Partitions();

		io.lettuce.core.cluster.models.partitions.RedisClusterNode partition = new io.lettuce.core.cluster.models.partitions.RedisClusterNode();
		partition.setNodeId(CLUSTER_NODE_1.getId());
		partition.setConnected(true);
		partition.setFlags(new HashSet<>(Arrays.asList(NodeFlag.MASTER, NodeFlag.MYSELF)));
		partition.setUri(RedisURI.create("redis://" + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT));
		partition.setSlots(Arrays.<Integer> asList(1, 2, 3, 4, 5));

		partitions.addPartition(partition);

		List<RedisClusterNode> nodes = LettuceConverters.partitionsToClusterNodes(partitions);
		assertThat(nodes.size(), is(1));

		RedisClusterNode node = nodes.get(0);
		assertThat(node.getHost(), is(CLUSTER_HOST));
		assertThat(node.getPort(), is(MASTER_NODE_1_PORT));
		assertThat(node.getFlags(), hasItems(Flag.MASTER, Flag.MYSELF));
		assertThat(node.getId(), is(CLUSTER_NODE_1.getId()));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));
		assertThat(node.getSlotRange().getSlots(), hasItems(1, 2, 3, 4, 5));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldReturnEmptyArgsForNullValues() {

		SetArgs args = LettuceConverters.toSetArgs(null, null);

		assertThat(getField(args, "ex"), is(nullValue()));
		assertThat(getField(args, "px"), is(nullValue()));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.FALSE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.FALSE));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldNotSetExOrPxForPersistent() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.persistent(), null);

		assertThat(getField(args, "ex"), is(nullValue()));
		assertThat(getField(args, "px"), is(nullValue()));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.FALSE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.FALSE));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldSetExForSeconds() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.seconds(10), null);

		assertThat((Long) getField(args, "ex"), is(10L));
		assertThat(getField(args, "px"), is(nullValue()));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.FALSE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.FALSE));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldSetPxForMilliseconds() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.milliseconds(100), null);

		assertThat(getField(args, "ex"), is(nullValue()));
		assertThat((Long) getField(args, "px"), is(100L));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.FALSE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.FALSE));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldSetNxForAbsent() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.ifAbsent());

		assertThat(getField(args, "ex"), is(nullValue()));
		assertThat(getField(args, "px"), is(nullValue()));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.TRUE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.FALSE));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldSetXxForPresent() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.ifPresent());

		assertThat(getField(args, "ex"), is(nullValue()));
		assertThat(getField(args, "px"), is(nullValue()));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.FALSE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.TRUE));
	}

	@Test // DATAREDIS-316
	public void toSetArgsShouldNotSetNxOrXxForUpsert() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.upsert());

		assertThat(getField(args, "ex"), is(nullValue()));
		assertThat(getField(args, "px"), is(nullValue()));
		assertThat((Boolean) getField(args, "nx"), is(Boolean.FALSE));
		assertThat((Boolean) getField(args, "xx"), is(Boolean.FALSE));
	}
}
