/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;

/**
 * Interface for the {@literal cluster} commands supported by Redis. A {@link RedisClusterNode} can be obtained from
 * {@link #clusterGetNodes()} or it can be constructed using either {@link RedisClusterNode#getHost() host} and
 * {@link RedisClusterNode#getPort()} or the {@link RedisClusterNode#getId() node Id}.
 * 
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public interface RedisClusterCommands {

	/**
	 * Retrieve cluster node information such as {@literal id}, {@literal host}, {@literal port} and {@literal slots}.
	 * 
	 * @return never {@literal null}.
	 */
	Iterable<RedisClusterNode> clusterGetNodes();

	/**
	 * Retrieve information about connected slaves for given master node.
	 * 
	 * @param master must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	Collection<RedisClusterNode> clusterGetSlaves(RedisClusterNode master);

	/**
	 * Retrieve information about masters and their connected slaves.
	 * 
	 * @return never {@literal null}.
	 */
	Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterSlaveMap();

	/**
	 * Find the slot for a given {@code key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Integer clusterGetSlotForKey(byte[] key);

	/**
	 * Find the {@link RedisClusterNode} serving given {@literal slot}.
	 * 
	 * @param slot
	 * @return
	 */
	RedisClusterNode clusterGetNodeForSlot(int slot);

	/**
	 * Find the {@link RedisClusterNode} serving given {@literal key}.
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	RedisClusterNode clusterGetNodeForKey(byte[] key);

	/**
	 * Get cluster information.
	 * 
	 * @return
	 */
	ClusterInfo clusterGetClusterInfo();

	/**
	 * Assign slots to given {@link RedisClusterNode}.
	 * 
	 * @param node must not be {@literal null}.
	 * @param slots
	 */
	void clusterAddSlots(RedisClusterNode node, int... slots);

	/**
	 * Assign {@link SlotRange#getSlotsArray()} to given {@link RedisClusterNode}.
	 * 
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 */
	void clusterAddSlots(RedisClusterNode node, SlotRange range);

	/**
	 * Count the number of keys assigned to one {@literal slot}.
	 * 
	 * @param slot
	 * @return
	 */
	Long clusterCountKeysInSlot(int slot);

	/**
	 * Remove slots from {@link RedisClusterNode}.
	 * 
	 * @param node must not be {@literal null}.
	 * @param slots
	 */
	void clusterDeleteSlots(RedisClusterNode node, int... slots);

	/**
	 * Removes {@link SlotRange#getSlotsArray()} from given {@link RedisClusterNode}.
	 * 
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 */
	void clusterDeleteSlotsInRange(RedisClusterNode node, SlotRange range);

	/**
	 * Remove given {@literal node} from cluster.
	 * 
	 * @param node must not be {@literal null}.
	 */
	void clusterForget(RedisClusterNode node);

	/**
	 * Add given {@literal node} to cluster.
	 *
	 * @param node must contain {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} and must
	 *          not be {@literal null}.
	 */
	void clusterMeet(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param slot
	 * @param mode must not be{@literal null}.
	 */
	void clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode);

	/**
	 * Get {@literal keys} served by slot.
	 * 
	 * @param slot
	 * @param count must not be {@literal null}.
	 * @return
	 */
	List<byte[]> clusterGetKeysInSlot(int slot, Integer count);

	/**
	 * Assign a {@literal slave} to given {@literal master}.
	 * 
	 * @param master must not be {@literal null}.
	 * @param slave must not be {@literal null}.
	 */
	void clusterReplicate(RedisClusterNode master, RedisClusterNode slave);

	public enum AddSlots {
		MIGRATING, IMPORTING, STABLE, NODE
	}

}
