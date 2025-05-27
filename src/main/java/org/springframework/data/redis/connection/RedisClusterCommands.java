/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
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
@NullUnmarked
public interface RedisClusterCommands {

	/**
	 * Retrieve cluster node information such as {@literal id}, {@literal host}, {@literal port} and {@literal slots}.
	 *
	 * @return never {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-nodes">Redis Documentation: CLUSTER NODES</a>
	 */
	Iterable<@NonNull RedisClusterNode> clusterGetNodes();

	/**
	 * Retrieve information about connected replicas for given master node.
	 *
	 * @param master must not be {@literal null}.
	 * @return never {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-replicas">Redis Documentation: CLUSTER REPLICAS</a>
	 */
	Collection<@NonNull RedisClusterNode> clusterGetReplicas(@NonNull RedisClusterNode master);

	/**
	 * Retrieve information about masters and their connected replicas.
	 *
	 * @return never {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-replicas">Redis Documentation: CLUSTER REPLICAS</a>
	 */
	Map<@NonNull RedisClusterNode, @NonNull Collection<@NonNull RedisClusterNode>> clusterGetMasterReplicaMap();

	/**
	 * Find the slot for a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/cluster-keyslot">Redis Documentation: CLUSTER KEYSLOT</a>
	 */
	Integer clusterGetSlotForKey(byte @NonNull [] key);

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
	RedisClusterNode clusterGetNodeForKey(byte @NonNull [] key);

	/**
	 * Get cluster information.
	 *
	 * @return
	 * @see <a href="https://redis.io/commands/cluster-info">Redis Documentation: CLUSTER INFO</a>
	 */
	ClusterInfo clusterGetClusterInfo();

	/**
	 * Assign slots to given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param slots
	 * @see <a href="https://redis.io/commands/cluster-addslots">Redis Documentation: CLUSTER ADDSLOTS</a>
	 */
	void clusterAddSlots(@NonNull RedisClusterNode node, int @NonNull... slots);

	/**
	 * Assign {@link SlotRange#getSlotsArray()} to given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-addslots">Redis Documentation: CLUSTER ADDSLOTS</a>
	 */
	void clusterAddSlots(@NonNull RedisClusterNode node, @NonNull SlotRange range);

	/**
	 * Count the number of keys assigned to one {@literal slot}.
	 *
	 * @param slot
	 * @return
	 * @see <a href="https://redis.io/commands/cluster-countkeysinslot">Redis Documentation: CLUSTER COUNTKEYSINSLOT</a>
	 */
	Long clusterCountKeysInSlot(int slot);

	/**
	 * Remove slots from {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param slots
	 * @see <a href="https://redis.io/commands/cluster-delslots">Redis Documentation: CLUSTER DELSLOTS</a>
	 */
	void clusterDeleteSlots( @NonNull RedisClusterNode node, int @NonNull ... slots);

	/**
	 * Removes {@link SlotRange#getSlotsArray()} from given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-delslots">Redis Documentation: CLUSTER DELSLOTS</a>
	 */
	void clusterDeleteSlotsInRange(@NonNull RedisClusterNode node, @NonNull SlotRange range);

	/**
	 * Remove given {@literal node} from cluster.
	 *
	 * @param node must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-forget">Redis Documentation: CLUSTER FORGET</a>
	 */
	void clusterForget(@NonNull RedisClusterNode node);

	/**
	 * Add given {@literal node} to cluster.
	 *
	 * @param node must contain {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} and must
	 *          not be {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-meet">Redis Documentation: CLUSTER MEET</a>
	 */
	void clusterMeet(@NonNull RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param slot
	 * @param mode must not be{@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-setslot">Redis Documentation: CLUSTER SETSLOT</a>
	 */
	void clusterSetSlot(@NonNull RedisClusterNode node, int slot, @NonNull AddSlots mode);

	/**
	 * Get {@literal keys} served by slot.
	 *
	 * @param slot
	 * @param count must not be {@literal null}.
	 * @return
	 * @see <a href="https://redis.io/commands/cluster-getkeysinslot">Redis Documentation: CLUSTER GETKEYSINSLOT</a>
	 */
	List<byte[]> clusterGetKeysInSlot(int slot, @NonNull Integer count);

	/**
	 * Assign a {@literal replica} to given {@literal master}.
	 *
	 * @param master must not be {@literal null}.
	 * @param replica must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-replicate">Redis Documentation: CLUSTER REPLICATE</a>
	 */
	void clusterReplicate(@NonNull RedisClusterNode master, @NonNull RedisClusterNode replica);

	enum AddSlots {
		MIGRATING, IMPORTING, STABLE, NODE
	}

}
