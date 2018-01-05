/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.Flag;
import org.springframework.data.redis.connection.RedisClusterNode.LinkState;
import org.springframework.data.redis.connection.RedisNode.NodeType;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ConvertersUnitTests {

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

	@Test // DATAREDIS-315
	public void toSetOfRedis30ClusterNodesShouldConvertSingleStringNodesResponseCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(REDIS_3_0_CLUSTER_NODES_RESPONSE).iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6379));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(0), is(true));
		assertThat(node.getSlotRange().contains(5460), is(true));
		assertThat(node.getSlotRange().contains(5461), is(false));
		assertThat(node.getSlotRange().contains(5602), is(true));
		assertThat(node.getFlags(), hasItems(Flag.MASTER, Flag.MYSELF));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));

		node = nodes.next(); // 127.0.0.1:6380
		assertThat(node.getId(), is("0f2ee5df45d18c50aca07228cc18b1da96fd5e84"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6380));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(5460), is(false));
		assertThat(node.getSlotRange().contains(5461), is(true));
		assertThat(node.getSlotRange().contains(5462), is(false));
		assertThat(node.getSlotRange().contains(10922), is(true));
		assertThat(node.getFlags(), hasItems(Flag.MASTER));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));

		node = nodes.next(); // 127.0.0.1:638
		assertThat(node.getId(), is("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6381));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(10923), is(true));
		assertThat(node.getSlotRange().contains(16383), is(true));
		assertThat(node.getFlags(), hasItems(Flag.MASTER));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));

		node = nodes.next(); // 127.0.0.1:7369
		assertThat(node.getId(), is("8cad73f63eb996fedba89f041636f17d88cda075"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(7369));
		assertThat(node.getType(), is(NodeType.SLAVE));
		assertThat(node.getMasterId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getSlotRange(), notNullValue());
		assertThat(node.getFlags(), hasItems(Flag.SLAVE));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));
	}

	@Test // DATAREDIS-315
	public void toSetOfRedis32ClusterNodesShouldConvertSingleStringNodesResponseCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(REDIS_3_2_CLUSTER_NODES_RESPONSE).iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6379));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(0), is(true));
		assertThat(node.getSlotRange().contains(5460), is(true));
		assertThat(node.getSlotRange().contains(5461), is(false));
		assertThat(node.getSlotRange().contains(5602), is(true));
		assertThat(node.getFlags(), hasItems(Flag.MASTER, Flag.MYSELF));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));

		node = nodes.next(); // 127.0.0.1:6380
		assertThat(node.getId(), is("0f2ee5df45d18c50aca07228cc18b1da96fd5e84"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6380));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(5460), is(false));
		assertThat(node.getSlotRange().contains(5461), is(true));
		assertThat(node.getSlotRange().contains(5462), is(false));
		assertThat(node.getSlotRange().contains(10922), is(true));
		assertThat(node.getFlags(), hasItems(Flag.MASTER));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));

		node = nodes.next(); // 127.0.0.1:638
		assertThat(node.getId(), is("3b9b8192a874fa8f1f09dbc0ee20afab5738eee7"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6381));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(10923), is(true));
		assertThat(node.getSlotRange().contains(16383), is(true));
		assertThat(node.getFlags(), hasItems(Flag.MASTER));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));

		node = nodes.next(); // 127.0.0.1:7369
		assertThat(node.getId(), is("8cad73f63eb996fedba89f041636f17d88cda075"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(7369));
		assertThat(node.getType(), is(NodeType.SLAVE));
		assertThat(node.getMasterId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getSlotRange(), notNullValue());
		assertThat(node.getFlags(), hasItems(Flag.SLAVE));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));
	}

	@Test // DATAREDIS-315
	public void toSetOfRedisClusterNodesShouldConvertNodesWithSingleSlotCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(CLUSTER_NODE_WITH_SINGLE_SLOT_RESPONSE)
				.iterator();

		RedisClusterNode node = nodes.next(); // 127.0.0.1:6379
		assertThat(node.getId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6379));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getSlotRange().contains(3456), is(true));
	}

	@Test // DATAREDIS-315
	public void toSetOfRedisClusterNodesShouldParseLinkStateAndDisconnectedCorrectly() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(
				CLUSTER_NODE_WITH_FAIL_FLAG_AND_DISCONNECTED_LINK_STATE).iterator();

		RedisClusterNode node = nodes.next();
		assertThat(node.getId(), is("b8b5ee73b1d1997abff694b3fe8b2397d2138b6d"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(7382));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getFlags(), hasItems(Flag.MASTER, Flag.FAIL));
		assertThat(node.getLinkState(), is(LinkState.DISCONNECTED));
		assertThat(node.getSlotRange().getSlots().size(), is(0));
	}

	@Test // DATAREDIS-315
	public void toSetOfRedisClusterNodesShouldIgnoreImportingSlot() {

		Iterator<RedisClusterNode> nodes = Converters.toSetOfRedisClusterNodes(CLUSTER_NODE_IMPORTING_SLOT).iterator();

		RedisClusterNode node = nodes.next();
		assertThat(node.getId(), is("ef570f86c7b1a953846668debc177a3a16733420"));
		assertThat(node.getHost(), is("127.0.0.1"));
		assertThat(node.getPort(), is(6379));
		assertThat(node.getType(), is(NodeType.MASTER));
		assertThat(node.getFlags(), hasItem(Flag.MASTER));
		assertThat(node.getLinkState(), is(LinkState.CONNECTED));
		assertThat(node.getSlotRange().getSlots().size(), is(0));
	}

}
