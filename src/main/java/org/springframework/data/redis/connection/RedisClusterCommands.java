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

import java.util.List;

/**
 * @author Christoph Strobl
 * @since 1.6
 */
public interface RedisClusterCommands {

	/**
	 * Retrieve cluster node information such as {@literal id}, {@literal host}, {@literal port} and {@literal slots}.
	 * 
	 * @return
	 */
	Iterable<RedisClusterNode> getClusterNodes();

	/**
	 * Retrieve information about connected slaves for given master node.
	 * 
	 * @param node
	 * @return
	 */
	Iterable<RedisClusterNode> getClusterSlaves(RedisNode master);

	/**
	 * Find the slot for a given {@code key}.
	 * 
	 * @param key
	 * @return
	 */
	Integer getClusterSlotForKey(byte[] key);

	/**
	 * Find the {@link RedisNode} serving given {@literal slot}.
	 * 
	 * @param slot
	 * @return
	 */
	RedisNode getClusterNodeForSlot(int slot);

	/**
	 * Find the {@link RedisNode} serving given {@literal key}.
	 * 
	 * @param key
	 * @return
	 */
	RedisNode getClusterNodeForKey(byte[] key);

	/**
	 * Get cluster information.
	 * 
	 * @return
	 */
	ClusterInfo getClusterInfo();

	/**
	 * Assign slots to given {@link RedisNode}.
	 * 
	 * @param node
	 * @param slots
	 */
	void addSlots(RedisNode node, int... slots);

	/**
	 * Count the number of keys assigned to one {@literal slot}.
	 * 
	 * @param slot
	 * @return
	 */
	Long countKeys(int slot);

	/**
	 * Remove slots from {@link RedisNode}.
	 * 
	 * @param node
	 * @param slots
	 */
	void deleteSlots(RedisNode node, int... slots);

	/**
	 * Remove given {@literal node} from cluster.
	 * 
	 * @param node
	 */
	void clusterForget(RedisNode node);

	/**
	 * Add given {@literal node} to cluster.
	 * 
	 * @param node
	 */
	void clusterMeet(RedisNode node);

	/**
	 * @param node
	 * @param slot
	 * @param mode
	 */
	void clusterSetSlot(RedisNode node, int slot, AddSlots mode);

	/**
	 * Get {@literal keys} served by slot.
	 * 
	 * @param slot
	 * @param count
	 * @return
	 */
	List<byte[]> getKeysInSlot(int slot, Integer count);

	/**
	 * Assign a {@literal slave} to given {@literal master}.
	 * 
	 * @param master
	 * @param slave
	 */
	void clusterReplicate(RedisNode master, RedisNode slave);

	public enum AddSlots {
		MIGRATING, IMPORTING, STABLE, NODE
	}

}
