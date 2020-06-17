/*
 * Copyright 2020 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;

/**
 * Interface for the {@literal cluster} commands supported by Redis executed using reactive infrastructure. A
 * {@link RedisClusterNode} can be obtained from {@link #clusterGetNodes()} or it can be constructed using either
 * {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} or the {@link RedisClusterNode#getId()
 * node Id}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.3.2
 */
public interface ReactiveClusterCommands {

	/**
	 * Retrieve cluster node information such as {@literal id}, {@literal host}, {@literal port} and {@literal slots}.
	 *
	 * @return a {@link Flux} emitting {@link RedisClusterNode cluster nodes}, an {@link Flux#empty() empty one} if none
	 *         found.
	 * @see <a href="https://redis.io/commands/cluster-nodes">Redis Documentation: CLUSTER NODES</a>
	 */
	Flux<RedisClusterNode> clusterGetNodes();

	/**
	 * Retrieve information about connected slaves for given master node.
	 *
	 * @param master must not be {@literal null}.
	 * @return a {@link Flux} emitting {@link RedisClusterNode cluster nodes}, an {@link Flux#empty() empty one} if none
	 *         found.
	 * @see <a href="https://redis.io/commands/cluster-slaves">Redis Documentation: CLUSTER SLAVES</a>
	 */
	Flux<RedisClusterNode> clusterGetSlaves(RedisClusterNode master);

	/**
	 * Retrieve information about masters and their connected slaves.
	 *
	 * @return never {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-slaves">Redis Documentation: CLUSTER SLAVES</a>
	 */
	Mono<Map<RedisClusterNode, Collection<RedisClusterNode>>> clusterGetMasterSlaveMap();

	/**
	 * Find the slot for a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a {@link Mono} emitting the calculated slog.
	 * @see <a href="https://redis.io/commands/cluster-keyslot">Redis Documentation: CLUSTER KEYSLOT</a>
	 */
	Mono<Integer> clusterGetSlotForKey(ByteBuffer key);

	/**
	 * Find the {@link RedisClusterNode} serving given {@literal slot}.
	 *
	 * @param slot
	 * @return a {@link Mono} emitting the {@link RedisClusterNode} handling the given slot.
	 */
	Mono<RedisClusterNode> clusterGetNodeForSlot(int slot);

	/**
	 * Find the {@link RedisClusterNode} serving given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return a {@link Mono} emitting the {@link RedisClusterNode} handling the slot for the given key.
	 */
	Mono<RedisClusterNode> clusterGetNodeForKey(ByteBuffer key);

	/**
	 * Get cluster information.
	 *
	 * @return never {@literal null}.
	 * @see <a href="https://redis.io/commands/cluster-info">Redis Documentation: CLUSTER INFO</a>
	 */
	Mono<ClusterInfo> clusterGetClusterInfo();

	/**
	 * Assign slots to given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param slots must not be empty.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-addslots">Redis Documentation: CLUSTER ADDSLOTS</a>
	 */
	Mono<Void> clusterAddSlots(RedisClusterNode node, int... slots);

	/**
	 * Assign {@link SlotRange#getSlotsArray()} to given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-addslots">Redis Documentation: CLUSTER ADDSLOTS</a>
	 */
	Mono<Void> clusterAddSlots(RedisClusterNode node, SlotRange range);

	/**
	 * Count the number of keys assigned to one {@literal slot}.
	 *
	 * @param slot
	 * @return a {@link Mono} emitting the number of keys stored at the given slot.
	 * @see <a href="https://redis.io/commands/cluster-countkeysinslot">Redis Documentation: CLUSTER COUNTKEYSINSLOT</a>
	 */
	Mono<Long> clusterCountKeysInSlot(int slot);

	/**
	 * Remove slots from {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-delslots">Redis Documentation: CLUSTER DELSLOTS</a>
	 */
	Mono<Void> clusterDeleteSlots(RedisClusterNode node, int... slots);

	/**
	 * Removes {@link SlotRange#getSlotsArray()} from given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param range must not be {@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-delslots">Redis Documentation: CLUSTER DELSLOTS</a>
	 */
	Mono<Void> clusterDeleteSlotsInRange(RedisClusterNode node, SlotRange range);

	/**
	 * Remove given {@literal node} from cluster.
	 *
	 * @param node must not be {@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-forget">Redis Documentation: CLUSTER FORGET</a>
	 */
	Mono<Void> clusterForget(RedisClusterNode node);

	/**
	 * Add given {@literal node} to cluster.
	 *
	 * @param node must contain {@link RedisClusterNode#getHost() host} and {@link RedisClusterNode#getPort()} and must
	 *          not be {@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-meet">Redis Documentation: CLUSTER MEET</a>
	 */
	Mono<Void> clusterMeet(RedisClusterNode node);

	/**
	 * @param node must not be {@literal null}.
	 * @param slot
	 * @param mode must not be{@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-setslot">Redis Documentation: CLUSTER SETSLOT</a>
	 */
	Mono<Void> clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode);

	/**
	 * Get {@literal keys} served by slot.
	 *
	 * @param slot
	 * @param count must not be {@literal null}.
	 * @return a {@link Flux} emitting the number of requested keys in the given slot, or signalling completion if none
	 *         found.
	 * @see <a href="https://redis.io/commands/cluster-getkeysinslot">Redis Documentation: CLUSTER GETKEYSINSLOT</a>
	 */
	Flux<ByteBuffer> clusterGetKeysInSlot(int slot, int count);

	/**
	 * Assign a {@literal slave} to given {@literal master}.
	 *
	 * @param master must not be {@literal null}.
	 * @param replica must not be {@literal null}.
	 * @return a {@link Mono} signaling completion.
	 * @see <a href="https://redis.io/commands/cluster-replicate">Redis Documentation: CLUSTER REPLICATE</a>
	 */
	Mono<Void> clusterReplicate(RedisClusterNode master, RedisClusterNode replica);

	enum AddSlots {
		MIGRATING, IMPORTING, STABLE, NODE
	}

}
