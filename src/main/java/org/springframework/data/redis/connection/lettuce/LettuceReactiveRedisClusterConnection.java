/*
 * Copyright 2016-2020 the original author or authors.
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

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.BaseRedisReactiveCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link ReactiveRedisClusterConnection} implementation for {@literal Lettuce}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveRedisClusterConnection extends LettuceReactiveRedisConnection
		implements ReactiveRedisClusterConnection {

	private final ClusterTopologyProvider topologyProvider;

	/**
	 * Creates new {@link LettuceReactiveRedisClusterConnection} given {@link LettuceConnectionProvider} and
	 * {@link RedisClusterClient}.
	 *
	 * @param connectionProvider must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 */
	LettuceReactiveRedisClusterConnection(LettuceConnectionProvider connectionProvider, RedisClusterClient client) {

		super(connectionProvider);

		this.topologyProvider = new LettuceClusterTopologyProvider(client);
	}

	/**
	 * Creates new {@link LettuceReactiveRedisClusterConnection} given a shared {@link StatefulConnection connection},
	 * {@link LettuceConnectionProvider} and {@link RedisClusterClient}.
	 *
	 * @param sharedConnection must not be {@literal null}.
	 * @param connectionProvider must not be {@literal null}.
	 * @param client must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code client} is {@literal null}.
	 * @since 2.0.1
	 */
	@SuppressWarnings("unchecked")
	LettuceReactiveRedisClusterConnection(StatefulConnection<ByteBuffer, ByteBuffer> sharedConnection,
			LettuceConnectionProvider connectionProvider, RedisClusterClient client) {

		super(sharedConnection, connectionProvider);

		this.topologyProvider = new LettuceClusterTopologyProvider(client);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#keyCommands()
	 */
	@Override
	public LettuceReactiveClusterKeyCommands keyCommands() {
		return new LettuceReactiveClusterKeyCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#listCommands()
	 */
	@Override
	public LettuceReactiveClusterListCommands listCommands() {
		return new LettuceReactiveClusterListCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#setCommands()
	 */
	@Override
	public LettuceReactiveClusterSetCommands setCommands() {
		return new LettuceReactiveClusterSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#zSetCommands()
	 */
	@Override
	public LettuceReactiveClusterZSetCommands zSetCommands() {
		return new LettuceReactiveClusterZSetCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#hyperLogLogCommands()
	 */
	@Override
	public LettuceReactiveClusterHyperLogLogCommands hyperLogLogCommands() {
		return new LettuceReactiveClusterHyperLogLogCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#stringCommands()
	 */
	@Override
	public LettuceReactiveClusterStringCommands stringCommands() {
		return new LettuceReactiveClusterStringCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#geoCommands()
	 */
	@Override
	public LettuceReactiveClusterGeoCommands geoCommands() {
		return new LettuceReactiveClusterGeoCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#hashCommands()
	 */
	@Override
	public LettuceReactiveClusterHashCommands hashCommands() {
		return new LettuceReactiveClusterHashCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#numberCommands()
	 */
	@Override
	public LettuceReactiveClusterNumberCommands numberCommands() {
		return new LettuceReactiveClusterNumberCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#scriptingCommands()
	 */
	@Override
	public LettuceReactiveClusterScriptingCommands scriptingCommands() {
		return new LettuceReactiveClusterScriptingCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#serverCommands()
	 */
	@Override
	public LettuceReactiveClusterServerCommands serverCommands() {
		return new LettuceReactiveClusterServerCommands(this, topologyProvider);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#streamCommands()
	 */
	@Override
	public LettuceReactiveClusterStreamCommands streamCommands() {
		return new LettuceReactiveClusterStreamCommands(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterConnection#ping()
	 */
	@Override
	public Mono<String> ping() {
		return clusterGetNodes().flatMap(node -> execute(node, BaseRedisReactiveCommands::ping)).last();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterConnection#ping(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Mono<String> ping(RedisClusterNode node) {
		return execute(node, BaseRedisReactiveCommands::ping).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetNodes()
	 */
	@Override
	public Flux<RedisClusterNode> clusterGetNodes() {
		return Flux.fromStream(() -> doGetActiveNodes().stream());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetSlaves(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Flux<RedisClusterNode> clusterGetSlaves(RedisClusterNode master) {

		Assert.notNull(master, "Master must not be null!");

		return Mono.fromSupplier(() -> lookup(master))
				.flatMapMany(nodeToUse -> execute(nodeToUse, cmd -> cmd.clusterSlaves(nodeToUse.getId()) //
						.flatMapIterable(LettuceConverters::toSetOfRedisClusterNodes)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetMasterSlaveMap()
	 */
	@Override
	public Mono<Map<RedisClusterNode, Collection<RedisClusterNode>>> clusterGetMasterSlaveMap() {

		return Flux.fromStream(() -> topologyProvider.getTopology().getActiveMasterNodes().stream()) //
				.flatMap(node -> {
					return Mono.just(node).zipWith(execute(node, cmd -> cmd.clusterSlaves(node.getId())) //
							.collectList() //
							.map(Converters::toSetOfRedisClusterNodes));
				}).collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetSlotForKey(java.nio.ByteBuffer)
	 */
	@Override
	public Mono<Integer> clusterGetSlotForKey(ByteBuffer key) {
		return Mono.fromSupplier(() -> SlotHash.getSlot(key));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetNodeForSlot(int)
	 */
	@Override
	public Mono<RedisClusterNode> clusterGetNodeForSlot(int slot) {

		Set<RedisClusterNode> nodes = topologyProvider.getTopology().getSlotServingNodes(slot);
		return nodes.isEmpty() ? Mono.empty() : Flux.fromIterable(nodes).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetNodeForKey(java.nio.ByteBuffer)
	 */
	@Override
	public Mono<RedisClusterNode> clusterGetNodeForKey(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null.");

		return clusterGetSlotForKey(key).flatMap(this::clusterGetNodeForSlot);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetClusterInfo()
	 */
	@Override
	public Mono<ClusterInfo> clusterGetClusterInfo() {

		return executeCommandOnArbitraryNode(RedisClusterReactiveCommands::clusterInfo) //
				.map(LettuceConverters::toProperties) //
				.map(ClusterInfo::new) //
				.single();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterAddSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public Mono<Void> clusterAddSlots(RedisClusterNode node, int... slots) {
		return execute(node, cmd -> cmd.clusterAddSlots(slots)).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterAddSlots(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode.SlotRange)
	 */
	@Override
	public Mono<Void> clusterAddSlots(RedisClusterNode node, RedisClusterNode.SlotRange range) {

		Assert.notNull(range, "Range must not be null.");

		return execute(node, cmd -> cmd.clusterAddSlots(range.getSlotsArray())).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterCountKeysInSlot(int)
	 */
	@Override
	public Mono<Long> clusterCountKeysInSlot(int slot) {
		return execute(cmd -> cmd.clusterCountKeysInSlot(slot)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterDeleteSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public Mono<Void> clusterDeleteSlots(RedisClusterNode node, int... slots) {
		return execute(node, cmd -> cmd.clusterDelSlots(slots)).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterDeleteSlotsInRange(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode.SlotRange)
	 */
	@Override
	public Mono<Void> clusterDeleteSlotsInRange(RedisClusterNode node, RedisClusterNode.SlotRange range) {

		Assert.notNull(range, "Range must not be null.");

		return execute(node, cmd -> cmd.clusterDelSlots(range.getSlotsArray())).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterForget(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Mono<Void> clusterForget(RedisClusterNode node) {

		RedisClusterNode nodeToRemove = lookup(node);

		return Flux.fromStream(() -> {

			List<RedisClusterNode> nodes = new ArrayList<>(doGetActiveNodes());
			nodes.remove(nodeToRemove);
			return nodes.stream();
		}).flatMap(actualNode -> execute(node, cmd -> cmd.clusterForget(nodeToRemove.getId()))).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterMeet(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Mono<Void> clusterMeet(RedisClusterNode node) {

		Assert.notNull(node, "Cluster node must not be null for CLUSTER MEET command!");
		Assert.hasText(node.getHost(), "Node to meet cluster must have a host!");
		Assert.isTrue(node.getPort() != null && node.getPort() > 0, "Node to meet cluster must have a port greater 0!");

		return clusterGetNodes()
				.flatMap(actualNode -> execute(node, cmd -> cmd.clusterMeet(node.getHost(), node.getPort()))).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterSetSlot(org.springframework.data.redis.connection.RedisClusterNode, int, org.springframework.data.redis.connection.ReactiveRedisClusterCommands.AddSlots)
	 */
	@Override
	public Mono<Void> clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode) {

		Assert.notNull(node, "Node must not be null.");
		Assert.notNull(mode, "AddSlots mode must not be null.");

		return execute(node, cmd -> {

			RedisClusterNode nodeToUse = lookup(node);
			String nodeId = nodeToUse.getId();

			switch (mode) {
				case MIGRATING:
					return cmd.clusterSetSlotMigrating(slot, nodeId);
				case IMPORTING:
					return cmd.clusterSetSlotImporting(slot, nodeId);
				case NODE:
					return cmd.clusterSetSlotNode(slot, nodeId);
				case STABLE:
					return cmd.clusterSetSlotStable(slot);
				default:
					throw new InvalidDataAccessApiUsageException("Invalid import mode for cluster slot: " + slot);
			}

		}).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterGetKeysInSlot(int, int)
	 */
	@Override
	public Flux<ByteBuffer> clusterGetKeysInSlot(int slot, int count) {
		return execute(cmd -> cmd.clusterGetKeysInSlot(slot, count));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisClusterCommands#clusterReplicate(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Mono<Void> clusterReplicate(RedisClusterNode master, RedisClusterNode replica) {
		return execute(replica, cmd -> cmd.clusterReplicate(lookup(master).getId())).then();
	}

	/**
	 * Run {@link LettuceReactiveCallback} on a random node.
	 *
	 * @param callback must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code node} or {@code callback} is {@literal null}.
	 * @return {@link Flux} emitting execution results.
	 */
	public <T> Flux<T> executeCommandOnArbitraryNode(LettuceReactiveCallback<T> callback) {

		Assert.notNull(callback, "ReactiveCallback must not be null!");

		return Mono.fromSupplier(() -> {

			List<RedisClusterNode> nodes = new ArrayList<>(doGetActiveNodes());
			int random = new Random().nextInt(nodes.size());

			return nodes.get(random);
		}).flatMapMany(it -> execute(it, callback));
	}

	/**
	 * Run {@link LettuceReactiveCallback} on given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @throws IllegalArgumentException when {@code node} or {@code callback} is {@literal null}.
	 * @return {@link Flux} emitting execution results.
	 */
	public <T> Flux<T> execute(RedisNode node, LettuceReactiveCallback<T> callback) {

		Assert.notNull(node, "RedisClusterNode must not be null!");
		Assert.notNull(callback, "ReactiveCallback must not be null!");

		return getCommands(node).flatMapMany(callback::doWithCommands).onErrorMap(translateException());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#getConnection()
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Mono<StatefulRedisClusterConnection<ByteBuffer, ByteBuffer>> getConnection() {
		return (Mono) super.getConnection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveRedisConnection#getCommands()
	 */
	protected Mono<RedisClusterReactiveCommands<ByteBuffer, ByteBuffer>> getCommands() {
		return getConnection().map(StatefulRedisClusterConnection::reactive);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Mono<RedisReactiveCommands<ByteBuffer, ByteBuffer>> getCommands(RedisNode node) {

		if (StringUtils.hasText(node.getId())) {
			return getConnection().cast(StatefulRedisClusterConnection.class)
					.map(it -> it.getConnection(node.getId()).reactive());
		}

		return getConnection().flatMap(it -> Mono.fromCompletionStage(it.getConnectionAsync(node.getHost(), node.getPort()))
				.map(StatefulRedisConnection::reactive));
	}

	/**
	 * Lookup a {@link RedisClusterNode} by using either ids {@link RedisClusterNode#getId() node id} or host and port to
	 * obtain the full node details from the underlying {@link ClusterTopologyProvider}.
	 *
	 * @param nodeToLookup the node to lookup.
	 * @return the {@link RedisClusterNode} from the topology lookup.
	 */
	private RedisClusterNode lookup(RedisClusterNode nodeToLookup) {
		return topologyProvider.getTopology().lookup(nodeToLookup);
	}

	private Set<RedisClusterNode> doGetActiveNodes() {
		return topologyProvider.getTopology().getActiveNodes();
	}
}
