/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.Set;

import org.springframework.data.redis.connection.RedisClusterCommands;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Redis operations for cluster specific operations. A {@link RedisClusterNode} can be obtained from
 * {@link RedisClusterCommands#clusterGetNodes() a connection} or it can be
 * constructed using either {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} or the
 * {@link RedisClusterNode#getId() node Id}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public interface ClusterOperations<K, V> {

	/**
	 * Get all keys located at given node.
	 *
	 * @param node must not be {@literal null}.
	 * @param pattern
	 * @return never {@literal null}.
	 * @see RedisConnection#keys(byte[])
	 */
	Set<K> keys(RedisClusterNode node, K pattern);

	/**
	 * Ping the given node;
	 *
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisConnection#ping()
	 */
	String ping(RedisClusterNode node);

	/**
	 * Get a random key from the range served by the given node.
	 *
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisConnection#randomKey()
	 */
	K randomKey(RedisClusterNode node);

	/**
	 * Add slots to given node;
	 *
	 * @param node must not be {@literal null}.
	 * @param slots must not be {@literal null}.
	 */
	void addSlots(RedisClusterNode node, int... slots);

	/**
	 * Add slots in {@link SlotRange} to given node.
	 *
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 */
	void addSlots(RedisClusterNode node, SlotRange range);

	/**
	 * Start an {@literal Append Only File} rewrite process on given node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#bgReWriteAof()
	 */
	void bgReWriteAof(RedisClusterNode node);

	/**
	 * Start background saving of db on given node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#bgSave()
	 */
	void bgSave(RedisClusterNode node);

	/**
	 * Add the node to cluster.
	 *
	 * @param node must not be {@literal null}.
	 */
	void meet(RedisClusterNode node);

	/**
	 * Remove the node from the cluster.
	 *
	 * @param node must not be {@literal null}.
	 */
	void forget(RedisClusterNode node);

	/**
	 * Flush db on node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#flushDb()
	 */
	void flushDb(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 */
	Collection<RedisClusterNode> getSlaves(RedisClusterNode node);

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#save()
	 */
	void save(RedisClusterNode node);

	/**
	 * Shutdown given node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#shutdown()
	 */
	void shutdown(RedisClusterNode node);

	/**
	 * Move slot assignment from one source to target node and copy keys associated with the slot.
	 *
	 * @param source must not be {@literal null}.
	 * @param slot
	 * @param target must not be {@literal null}.
	 */
	void reshard(RedisClusterNode source, int slot, RedisClusterNode target);
}
