/*
 * Copyright 2014-2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;
import static org.springframework.data.redis.connection.lettuce.LettuceCommandArgsComparator.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import io.lettuce.core.GetExArgs;
import io.lettuce.core.Limit;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode.NodeFlag;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.RedisClientInfo;

/**
 * Unit tests for {@link LettuceConverters}.
 *
 * @author Christoph Strobl
 * @author Vikas Garg
 */
class LettuceConvertersUnitTests {

	private static final String CLIENT_ALL_SINGLE_LINE_RESPONSE = "addr=127.0.0.1:60311 fd=6 name= age=4059 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

	private static final String MASTER_NAME = "mymaster";

	@Test // DATAREDIS-268
	void convertingEmptyStringToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(""))
				.isEqualTo(Collections.<RedisClientInfo> emptyList());
	}

	@Test // DATAREDIS-268
	void convertingNullToListOfRedisClientInfoShouldReturnEmptyList() {
		assertThat(LettuceConverters.toListOfRedisClientInformation(null))
				.isEqualTo(Collections.<RedisClientInfo> emptyList());
	}

	@Test // DATAREDIS-268
	void convertingMultipleLiesToListOfRedisClientInfoReturnsListCorrectly() {

		StringBuilder sb = new StringBuilder();
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);
		sb.append("\r\n");
		sb.append(CLIENT_ALL_SINGLE_LINE_RESPONSE);

		assertThat(LettuceConverters.toListOfRedisClientInformation(sb.toString()).size()).isEqualTo(2);
	}

	@Test // DATAREDIS-315
	void partitionsToClusterNodesShouldReturnEmptyCollectionWhenPartitionsDoesNotContainElements() {
		assertThat(LettuceConverters.partitionsToClusterNodes(new Partitions())).isNotNull();
	}

	@Test // DATAREDIS-315
	void partitionsToClusterNodesShouldConvertPartitionCorrectly() {

		Partitions partitions = new Partitions();

		io.lettuce.core.cluster.models.partitions.RedisClusterNode partition = new io.lettuce.core.cluster.models.partitions.RedisClusterNode();
		partition.setNodeId(CLUSTER_NODE_1.getId());
		partition.setConnected(true);
		partition.setFlags(new HashSet<>(Arrays.asList(NodeFlag.MASTER, NodeFlag.MYSELF)));
		partition.setUri(RedisURI.create("redis://" + CLUSTER_HOST + ":" + MASTER_NODE_1_PORT));
		partition.setSlots(Arrays.asList(1, 2, 3, 4, 5));

		partitions.add(partition);

		List<RedisClusterNode> nodes = LettuceConverters.partitionsToClusterNodes(partitions);
		assertThat(nodes.size()).isEqualTo(1);

		RedisClusterNode node = nodes.get(0);
		assertThat(node.getHost()).isEqualTo(CLUSTER_HOST);
		assertThat(node.getPort()).isEqualTo(MASTER_NODE_1_PORT);
		assertThat(node.getFlags()).contains(Flag.MASTER, Flag.MYSELF);
		assertThat(node.getId()).isEqualTo(CLUSTER_NODE_1.getId());
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots()).contains(1, 2, 3, 4, 5);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldReturnEmptyArgsForNullValues() {

		SetArgs args = LettuceConverters.toSetArgs(null, null);

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldNotSetExOrPxForPersistent() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.persistent(), null);

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetExForSeconds() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.seconds(10), null);

		assertThat((Long) getField(args, "ex")).isEqualTo(10L);
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // GH-2050
	void convertsExpirationToSetPXAT() {

		assertThatCommandArgument(LettuceConverters.toSetArgs(Expiration.unixTimestamp(10, TimeUnit.MILLISECONDS), null))
				.isEqualTo(SetArgs.Builder.pxAt(10));
	}

	@Test // GH-2050
	void convertsExpirationToSetEXAT() {

		assertThatCommandArgument(LettuceConverters.toSetArgs(Expiration.unixTimestamp(1, TimeUnit.MINUTES), null))
				.isEqualTo(SetArgs.Builder.exAt(60));
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetPxForMilliseconds() {

		SetArgs args = LettuceConverters.toSetArgs(Expiration.milliseconds(100), null);

		assertThat(getField(args, "ex")).isNull();
		assertThat((Long) getField(args, "px")).isEqualTo(100L);
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetNxForAbsent() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.ifAbsent());

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.TRUE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldSetXxForPresent() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.ifPresent());

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.TRUE);
	}

	@Test // DATAREDIS-316
	void toSetArgsShouldNotSetNxOrXxForUpsert() {

		SetArgs args = LettuceConverters.toSetArgs(null, SetOption.upsert());

		assertThat(getField(args, "ex")).isNull();
		assertThat(getField(args, "px")).isNull();
		assertThat((Boolean) getField(args, "nx")).isEqualTo(Boolean.FALSE);
		assertThat((Boolean) getField(args, "xx")).isEqualTo(Boolean.FALSE);
	}

	@Test // DATAREDIS-981
	void toLimit() {

		Limit limit = LettuceConverters.toLimit(RedisZSetCommands.Limit.unlimited());
		assertThat(limit.isLimited()).isFalse();
		assertThat(limit.getCount()).isEqualTo(-1L);

		limit = LettuceConverters.toLimit(RedisZSetCommands.Limit.limit().count(-1));
		assertThat(limit.isLimited()).isTrue();
		assertThat(limit.getCount()).isEqualTo(-1L);

		limit = LettuceConverters.toLimit(RedisZSetCommands.Limit.limit().count(5));
		assertThat(limit.isLimited()).isTrue();
		assertThat(limit.getCount()).isEqualTo(5L);
	}

	@Test // GH-2050
	void convertsExpirationToGetExEX() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.seconds(10))).isEqualTo(new GetExArgs().ex(10));
	}

	@Test // GH-2050
	void convertsExpirationWithTimeUnitToGetExEX() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.from(1, TimeUnit.MINUTES)))
				.isEqualTo(new GetExArgs().ex(60));
	}

	@Test // GH-2050
	void convertsExpirationToGetExPEX() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.milliseconds(10)))
				.isEqualTo(new GetExArgs().px(10));
	}

	@Test // GH-2050
	void convertsExpirationToGetExEXAT() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.unixTimestamp(10, TimeUnit.SECONDS)))
				.isEqualTo(new GetExArgs().exAt(10));
	}

	@Test // GH-2050
	void convertsExpirationWithTimeUnitToGetExEXAT() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.unixTimestamp(1, TimeUnit.MINUTES)))
				.isEqualTo(new GetExArgs().exAt(60));
	}

	@Test // GH-2050
	void convertsExpirationToGetExPXAT() {

		assertThatCommandArgument(LettuceConverters.toGetExArgs(Expiration.unixTimestamp(10, TimeUnit.MILLISECONDS)))
				.isEqualTo(new GetExArgs().pxAt(10));
	}

	@Test // GH-2218
	void sentinelConfigurationWithAuth() {

		RedisPassword dataPassword = RedisPassword.of("data-secret");
		RedisPassword sentinelPassword = RedisPassword.of("sentinel-secret");

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration()
				.master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379)
				.sentinel("127.0.0.1", 26380);
		sentinelConfiguration.setUsername("app");
		sentinelConfiguration.setPassword(dataPassword);

		sentinelConfiguration.setSentinelUsername("admin");
		sentinelConfiguration.setSentinelPassword(sentinelPassword);

		RedisURI redisURI = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);

		assertThat(redisURI.getUsername()).isEqualTo("app");
		assertThat(redisURI.getPassword()).isEqualTo(dataPassword.get());

		redisURI.getSentinels().forEach(sentinel -> {
			assertThat(sentinel.getUsername()).isEqualTo("admin");
			assertThat(sentinel.getPassword()).isEqualTo(sentinelPassword.get());
		});
	}

	@Test // GH-2218
	void sentinelConfigurationSetSentinelPasswordIfUsernameNotPresent() {

		RedisPassword password = RedisPassword.of("88888888-8x8-getting-creative-now");

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration()
				.master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379)
				.sentinel("127.0.0.1", 26380);
		sentinelConfiguration.setUsername("app");
		sentinelConfiguration.setPassword(password);
		sentinelConfiguration.setSentinelPassword(password);

		RedisURI redisURI = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);

		assertThat(redisURI.getUsername()).isEqualTo("app");

		redisURI.getSentinels().forEach(sentinel -> {
 			assertThat(sentinel.getUsername()).isNull();
			assertThat(sentinel.getPassword()).isNotNull();
		});
	}

	@Test // GH-2218
	void sentinelConfigurationShouldNotSetSentinelAuthIfUsernameIsPresentWithNoPassword() {

		RedisPassword password = RedisPassword.of("88888888-8x8-getting-creative-now");

		RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration()
				.master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379)
				.sentinel("127.0.0.1", 26380);
		sentinelConfiguration.setUsername("app");
		sentinelConfiguration.setPassword(password);
		sentinelConfiguration.setSentinelUsername("admin");

		RedisURI redisURI = LettuceConverters.sentinelConfigurationToRedisURI(sentinelConfiguration);

		assertThat(redisURI.getUsername()).isEqualTo("app");

		redisURI.getSentinels().forEach(sentinel -> {
			assertThat(sentinel.getUsername()).isNull();
			assertThat(sentinel.getPassword()).isNull();
		});
	}
}
