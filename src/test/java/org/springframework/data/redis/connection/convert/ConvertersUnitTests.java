/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import static org.assertj.core.api.Assertions.*;

import java.util.Iterator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisNode.NodeType;
import org.springframework.data.redis.connection.convert.Converters.ClusterNodesConverter.AddressPortHostname;

/**
 * Unit tests for {@link Converters}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Sorokin Evgeniy
 * @author Marcin Grzejszczak
 */
class ConvertersUnitTests {

	private static final String REDIS_3_0_CLUSTER_NODES_RESPONSE = "" //
			+ "ef570f86c7b1a953846668debc177a3a16733420 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460 5602"
			+ "\n"
			+ "0f2ee5df45d18c50aca07228cc18b1da96fd5e84 127.0.0.1:6380@16380 master - 0 1427718161587 2 connected 5461 5603-10922"
			+ "\n"
			+ "3b9b8192a874fa8f1f09dbc0ee20afab5738eee7 127.0.0.1:6381@6381 master - 0 1427718161587 3 connected 10923-16383"
			+ "\n"
			+ "8cad73f63eb996fedba89f041636f17d88cda075 127.0.0.1:7369@7369 slave ef570f86c7b1a953846668debc177a3a16733420 0 1427718161587 1 connected";

	private static final String REDIS_3_2_CLUSTER_NODES_RESPONSE = "" //
			+ "ef570f86c7b1a953846668debc177a3a16733420 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460 5602"
			+ "\n"
			+ "0f2ee5df45d18c50aca07228cc18b1da96fd5e84 127.0.0.1:6380@16380 master - 0 1427718161587 2 connected 5461 5603-10922"
			+ "\n"
			+ "3b9b8192a874fa8f1f09dbc0ee20afab5738eee7 127.0.0.1:6381@6381 master - 0 1427718161587 3 connected 10923-16383"
			+ "\n"
			+ "8cad73f63eb996fedba89f041636f17d88cda075 127.0.0.1:7369@7369 slave ef570f86c7b1a953846668debc177a3a16733420 0 1427718161587 1 connected";

	private static final String CLUSTER_NODE_WITH_SINGLE_SLOT_RESPONSE = "ef570f86c7b1a953846668debc177a3a16733420 127.0.0.1:6379 myself,master - 0 0 1 connected 3456";

	private static final String CLUSTER_NODE_WITH_FAIL_FLAG_AND_DISCONNECTED_LINK_STATE = "b8b5ee73b1d1997abff694b3fe8b2397d2138b6d 127.0.0.1:7382 master,fail - 1450160048933 1450160048832 38 disconnected";

	private static final String CLUSTER_NODE_IMPORTING_SLOT = "ef570f86c7b1a953846668debc177a3a16733420 127.0.0.1:6379 myself,master - 0 0 1 connected [5461-<-0f2ee5df45d18c50aca07228cc18b1da96fd5e84]";

	private static final String CLUSTER_NODE_WITHOUT_HOST = "ef570f86c7b1a953846668debc177a3a16733420 :6379 fail,master - 0 0 1 connected";

	private static final String CLUSTER_NODE_WITH_SINGLE_IPV6_HOST = "67adfe3df1058896e3cb49d2863e0f70e7e159fa 2a02:6b8:c67:9c:0:6d8b:33da:5a2c:6380@16380,redis-master master,nofailover - 0 1692108412315 1 connected 0-5460";

	private static final String CLUSTER_NODE_WITH_SINGLE_IPV6_HOST_SQUARE_BRACKETS = "67adfe3df1058896e3cb49d2863e0f70e7e159fa [2a02:6b8:c67:9c:0:6d8b:33da:5a2c]:6380@16380,redis-master master,nofailover - 0 1692108412315 1 connected 0-5460";

	private static final String CLUSTER_NODE_WITH_SINGLE_INVALID_IPV6_HOST = "67adfe3df1058896e3cb49d2863e0f70e7e159fa 2a02:6b8:c67:9c:0:6d8b:33da:5a2c: master,nofailover - 0 1692108412315 1 connected 0-5460";

	private static final String CLUSTER_NODE_WITH_SINGLE_IPV4_EMPTY_HOSTNAME = "3765733728631672640db35fd2f04743c03119c6 10.180.0.33:11003@16379, master - 0 1708041426947 2 connected 0-5460";

	private static final String CLUSTER_NODE_WITH_SINGLE_IPV4_HOSTNAME = "3765733728631672640db35fd2f04743c03119c6 10.180.0.33:11003@16379,hostname1 master - 0 1708041426947 2 connected 0-5460";

	@Test // DATAREDIS-315
	void toSetOfRedis30ClusterNodesShouldConvertSingleStringNodesResponseCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(REDIS_3_0_CLUSTER_NODES_RESPONSE).iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6379);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(0)).isTrue();
		assertThat(node.getSlotRange().contains(5460)).isTrue();
		assertThat(node.getSlotRange().contains(5461)).isFalse();
		assertThat(node.getSlotRange().contains(5602)).isTrue();
		assertThat(node.getFlags()).contains(Flag.MASTER, Flag.MYSELF);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);

		node = nodes.next(); // 127.0.0.1:6380
		assertThat(node.getId()).isEqualTo("0f2ee5df45d18c50aca07228cc18b1da96fd5e84");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6380);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(5460)).isFalse();
		assertThat(node.getSlotRange().contains(5461)).isTrue();
		assertThat(node.getSlotRange().contains(5462)).isFalse();
		assertThat(node.getSlotRange().contains(10922)).isTrue();
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);

		node = nodes.next(); // 127.0.0.1:638
		assertThat(node.getId()).isEqualTo("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6381);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(10923)).isTrue();
		assertThat(node.getSlotRange().contains(16383)).isTrue();
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);

		node = nodes.next(); // 127.0.0.1:7369
		assertThat(node.getId()).isEqualTo("8cad73f63eb996fedba89f041636f17d88cda075");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(7369);
		assertThat(node.getType()).isEqualTo(NodeType.REPLICA);
		assertThat(node.getMasterId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getSlotRange()).isNotNull();
		assertThat(node.getFlags()).contains(Flag.REPLICA);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
	}

	@Test // DATAREDIS-315
	void toSetOfRedis32ClusterNodesShouldConvertSingleStringNodesResponseCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(REDIS_3_2_CLUSTER_NODES_RESPONSE).iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6379);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(0)).isTrue();
		assertThat(node.getSlotRange().contains(5460)).isTrue();
		assertThat(node.getSlotRange().contains(5461)).isFalse();
		assertThat(node.getSlotRange().contains(5602)).isTrue();
		assertThat(node.getFlags()).contains(Flag.MASTER, Flag.MYSELF);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);

		node = nodes.next(); // 127.0.0.1:6380
		assertThat(node.getId()).isEqualTo("0f2ee5df45d18c50aca07228cc18b1da96fd5e84");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6380);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(5460)).isFalse();
		assertThat(node.getSlotRange().contains(5461)).isTrue();
		assertThat(node.getSlotRange().contains(5462)).isFalse();
		assertThat(node.getSlotRange().contains(10922)).isTrue();
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);

		node = nodes.next(); // 127.0.0.1:638
		assertThat(node.getId()).isEqualTo("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6381);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(10923)).isTrue();
		assertThat(node.getSlotRange().contains(16383)).isTrue();
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);

		node = nodes.next(); // 127.0.0.1:7369
		assertThat(node.getId()).isEqualTo("8cad73f63eb996fedba89f041636f17d88cda075");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(7369);
		assertThat(node.getType()).isEqualTo(NodeType.REPLICA);
		assertThat(node.getMasterId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getSlotRange()).isNotNull();
		assertThat(node.getFlags()).contains(Flag.REPLICA);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
	}

	@Test // DATAREDIS-315
	void toSetOfRedisClusterNodesShouldConvertNodesWithSingleSlotCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(CLUSTER_NODE_WITH_SINGLE_SLOT_RESPONSE)
				.iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(6379);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getSlotRange().contains(3456)).isTrue();
	}

	@Test // DATAREDIS-315
	void toSetOfRedisClusterNodesShouldParseLinkStateAndDisconnectedCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters
				.toSetOfRedisClusterNodes(CLUSTER_NODE_WITH_FAIL_FLAG_AND_DISCONNECTED_LINK_STATE).iterator();

		RedisClusterNode node = nodes.next();
		assertThat(node.getId()).isEqualTo("b8b5ee73b1d1997abff694b3fe8b2397d2138b6d");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.getPort()).isEqualTo(7382);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER, Flag.FAIL);
		assertThat(node.getLinkState()).isEqualTo(LinkState.DISCONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(0);
	}

	@Test // DATAREDIS-315
	void toSetOfRedisClusterNodesShouldIgnoreImportingSlot() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(CLUSTER_NODE_IMPORTING_SLOT).iterator();

		RedisClusterNode node = nodes.next();
		assertThat(node.getId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getHost()).isEqualTo("127.0.0.1");
		assertThat(node.hasValidHost()).isTrue();
		assertThat(node.getPort()).isEqualTo(6379);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(0);
	}

	@Test // GH-1985
	void toSetOfRedisClusterNodesShouldAllowEmptyHostname() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(CLUSTER_NODE_WITHOUT_HOST).iterator();

		RedisClusterNode node = nodes.next();
		assertThat(node.getId()).isEqualTo("ef570f86c7b1a953846668debc177a3a16733420");
		assertThat(node.getHost()).isEmpty();
		assertThat(node.hasValidHost()).isFalse();
		assertThat(node.getPort()).isEqualTo(6379);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(0);
	}

	@Test // https://github.com/spring-projects/spring-data-redis/issues/2678
	void toClusterNodeWithIPv6Hostname() {
		RedisClusterNode node = Converters.toClusterNode(CLUSTER_NODE_WITH_SINGLE_IPV6_HOST);

		assertThat(node.getId()).isEqualTo("67adfe3df1058896e3cb49d2863e0f70e7e159fa");
		assertThat(node.getHost()).isEqualTo("2a02:6b8:c67:9c:0:6d8b:33da:5a2c");
		assertThat(node.hasValidHost()).isTrue();
		assertThat(node.getPort()).isEqualTo(6380);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(5461);
	}

	@Test // GH-2862
	void toClusterNodeWithIPv4EmptyHostname() {

		RedisClusterNode node = Converters.toClusterNode(CLUSTER_NODE_WITH_SINGLE_IPV4_EMPTY_HOSTNAME);

		assertThat(node.getId()).isEqualTo("3765733728631672640db35fd2f04743c03119c6");
		assertThat(node.getHost()).isEqualTo("10.180.0.33");
		assertThat(node.hasValidHost()).isTrue();
		assertThat(node.getPort()).isEqualTo(11003);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(5461);
	}

	@Test // GH-2862
	void toClusterNodeWithIPv4Hostname() {

		RedisClusterNode node = Converters.toClusterNode(CLUSTER_NODE_WITH_SINGLE_IPV4_HOSTNAME);

		assertThat(node.getId()).isEqualTo("3765733728631672640db35fd2f04743c03119c6");
		assertThat(node.getHost()).isEqualTo("10.180.0.33");
		assertThat(node.getName()).isEqualTo("hostname1");
		assertThat(node.hasValidHost()).isTrue();
		assertThat(node.getPort()).isEqualTo(11003);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(5461);
	}

	@Test // GH-2678
	void toClusterNodeWithIPv6HostnameSquareBrackets() {

		RedisClusterNode node = Converters.toClusterNode(CLUSTER_NODE_WITH_SINGLE_IPV6_HOST_SQUARE_BRACKETS);

		assertThat(node.getId()).isEqualTo("67adfe3df1058896e3cb49d2863e0f70e7e159fa");
		assertThat(node.getHost()).isEqualTo("2a02:6b8:c67:9c:0:6d8b:33da:5a2c");
		assertThat(node.hasValidHost()).isTrue();
		assertThat(node.getPort()).isEqualTo(6380);
		assertThat(node.getType()).isEqualTo(NodeType.MASTER);
		assertThat(node.getFlags()).contains(Flag.MASTER);
		assertThat(node.getLinkState()).isEqualTo(LinkState.CONNECTED);
		assertThat(node.getSlotRange().getSlots().size()).isEqualTo(5461);
	}

	@Test // GH-2678
	void toClusterNodeWithInvalidIPv6Hostname() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> Converters.toClusterNode(CLUSTER_NODE_WITH_SINGLE_INVALID_IPV6_HOST));
	}

	@ParameterizedTest // GH-2678
	@MethodSource("clusterNodesEndpoints")
	void shouldAcceptHostPatterns(String endpoint, AddressPortHostname expected) {

		AddressPortHostname addressPortHostname = AddressPortHostname.parse(endpoint);

		assertThat(addressPortHostname).isEqualTo(expected);
	}

	static Stream<Arguments> clusterNodesEndpoints() {

		Stream<Arguments> regular = Stream.of(
				// IPv4 with Host, Redis 3
				Arguments.of("1.2.4.4:7379", new AddressPortHostname("1.2.4.4", "7379", null)),
				// IPv6 with Host, Redis 3
				Arguments.of("6b8:c67:9c:0:6d8b:33da:5a2c:6380",
						new AddressPortHostname("6b8:c67:9c:0:6d8b:33da:5a2c", "6380", null)),
				// Assuming IPv6 in brackets with Host, Redis 3
				Arguments.of("[6b8:c67:9c:0:6d8b:33da:5a2c]:6380",
						new AddressPortHostname("6b8:c67:9c:0:6d8b:33da:5a2c", "6380", null)),

				// IPv4 with Host and Bus Port, Redis 4
				Arguments.of("127.0.0.1:7382@17382", new AddressPortHostname("127.0.0.1", "7382", null)),
				// IPv6 with Host and Bus Port, Redis 4
				Arguments.of("6b8:c67:9c:0:6d8b:33da:5a2c:6380",
						new AddressPortHostname("6b8:c67:9c:0:6d8b:33da:5a2c", "6380", null)),

				// Hostname with Port and Bus Port, Redis 7
				Arguments.of("my.host-name.com:7379@17379", new AddressPortHostname("my.host-name.com", "7379", null)),

				// With hostname, Redis 7
				Arguments.of("1.2.4.4:7379@17379,my.host-name.com",
						new AddressPortHostname("1.2.4.4", "7379", "my.host-name.com")));

		Stream<Arguments> weird = Stream.of(
				// Port-only
				Arguments.of(":6380", new AddressPortHostname("", "6380", null)),

				// Port-only with bus-port
				Arguments.of(":6380@6381", new AddressPortHostname("", "6380", null)),
				// IP with trailing comma
				Arguments.of("127.0.0.1:6380,", new AddressPortHostname("127.0.0.1", "6380", null)),
				// IPv6 with bus-port
				Arguments.of("2a02:6b8:c67:9c:0:6d8b:33da:5a2c:6380@6381",
						new AddressPortHostname("2a02:6b8:c67:9c:0:6d8b:33da:5a2c", "6380", null)),
				// IPv6 with bus-port and hostname
				Arguments.of("2a02:6b8:c67:9c:0:6d8b:33da:5a2c:6380@6381,hostname1",
						new AddressPortHostname("2a02:6b8:c67:9c:0:6d8b:33da:5a2c", "6380", "hostname1")),
				// Port-only with hostname
				Arguments.of(":6380,hostname1", new AddressPortHostname("", "6380", "hostname1")),

				// Port-only with bus-port
				Arguments.of(":6380@6381,hostname1", new AddressPortHostname("", "6380", "hostname1")),
				// IPv6 in brackets with bus-port
				Arguments.of("[2a02:6b8:c67:9c:0:6d8b:33da:5a2c]:6380@6381",
						new AddressPortHostname("2a02:6b8:c67:9c:0:6d8b:33da:5a2c", "6380", null)),
				// IPv6 in brackets with bus-port and hostname
				Arguments.of("[2a02:6b8:c67:9c:0:6d8b:33da:5a2c]:6380@6381,hostname1",
						new AddressPortHostname("2a02:6b8:c67:9c:0:6d8b:33da:5a2c", "6380", "hostname1")));

		return Stream.concat(regular, weird);
	}
}
