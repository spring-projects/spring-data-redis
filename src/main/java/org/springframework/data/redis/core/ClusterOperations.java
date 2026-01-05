/*
 * Copyright 2015-present the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.RedisClusterCommands;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;

/**
 * Redis operations for cluster specific operations. A {@link RedisClusterNode} can be obtained from
 * {@link RedisClusterCommands#clusterGetNodes() a connection} or it can be constructed using either
 * {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} or the {@link RedisClusterNode#getId()
 * node Id}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Dennis Neufeld
 * @since 1.7
 */
@NullUnmarked
public interface ClusterOperations<K, V> {

	/**
	 * Retrieve all keys located at given node matching the given pattern.
	 * <p>
	 * <strong>IMPORTANT:</strong> The {@literal KEYS} command is non-interruptible and scans the entire keyspace which
	 * may cause performance issues.
	 *
	 * @param node must not be {@literal null}.
	 * @param pattern
	 * @return never {@literal null}.
	 * @see RedisConnection#keys(byte[])
	 */
	Set<@NonNull K> keys(@NonNull RedisClusterNode node, @NonNull K pattern);

	/**
	 * Ping the given node;
	 *
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisConnection#ping()
	 */
	String ping(@NonNull RedisClusterNode node);

	/**
	 * Get a random key from the range served by the given node.
	 *
	 * @param node must not be {@literal null}.
	 * @return
	 * @see RedisConnection#randomKey()
	 */
	K randomKey(@NonNull RedisClusterNode node);

	/**
	 * Add slots to given node;
	 *
	 * @param node must not be {@literal null}.
	 * @param slots must not be {@literal null}.
	 */
	void addSlots(@NonNull RedisClusterNode node, int... slots);

	/**
	 * Add slots in {@link SlotRange} to given node.
	 *
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 */
	void addSlots(@NonNull RedisClusterNode node, @NonNull SlotRange range);

	/**
	 * Start an {@literal Append Only File} rewrite process on given node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#bgReWriteAof()
	 */
	void bgReWriteAof(@NonNull RedisClusterNode node);

	/**
	 * Start background saving of db on given node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#bgSave()
	 */
	void bgSave(@NonNull RedisClusterNode node);

	/**
	 * Add the node to cluster.
	 *
	 * @param node must not be {@literal null}.
	 */
	void meet(@NonNull RedisClusterNode node);

	/**
	 * Remove the node from the cluster.
	 *
	 * @param node must not be {@literal null}.
	 */
	void forget(@NonNull RedisClusterNode node);

	/**
	 * Flush db on node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#flushDb()
	 */
	void flushDb(@NonNull RedisClusterNode node);

	/**
	 * Flush db on node using the specified {@link FlushOption}.
	 *
	 * @param node must not be {@literal null}.
	 * @param option must not be {@literal null}.
	 * @see RedisConnection#flushDb(FlushOption)
	 * @since 2.7
	 */
	void flushDb(@NonNull RedisClusterNode node, @NonNull FlushOption option);

	/**
	 * @param node must not be {@literal null}.
	 * @return
	 */
	Collection<@NonNull RedisClusterNode> getReplicas(@NonNull RedisClusterNode node);

	/**
	 * Synchronous save current db snapshot on server.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#save()
	 */
	void save(@NonNull RedisClusterNode node);

	/**
	 * Shutdown given node.
	 *
	 * @param node must not be {@literal null}.
	 * @see RedisConnection#shutdown()
	 */
	void shutdown(@NonNull RedisClusterNode node);

	/**
	 * Move slot assignment from one source to target node and copy keys associated with the slot.
	 *
	 * @param source must not be {@literal null}.
	 * @param slot
	 * @param target must not be {@literal null}.
	 */
	void reshard(@NonNull RedisClusterNode source, int slot, @NonNull RedisClusterNode target);
}
