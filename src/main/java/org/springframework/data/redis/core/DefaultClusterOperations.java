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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;
import org.springframework.util.Assert;

/**
 * Default {@link ClusterOperations} implementation.
 *
 * @author Christoph Strobl
 * @author Dennis Neufeld
 * @since 1.7
 * @param <K>
 * @param <V>
 */
@NullUnmarked
class DefaultClusterOperations<K, V> extends AbstractOperations<K, V> implements ClusterOperations<K, V> {

	private final RedisTemplate<K, V> template;

	/**
	 * Creates new {@link DefaultClusterOperations} delegating to the given {@link RedisTemplate}.
	 *
	 * @param template must not be {@literal null}.
	 */
	DefaultClusterOperations(@NonNull RedisTemplate<K, V> template) {

		super(template);
		this.template = template;
	}

	@Override
	public Set<K> keys(@NonNull RedisClusterNode node, @NonNull K pattern) {

		Assert.notNull(node, "ClusterNode must not be null");

		return doInCluster(connection -> deserializeKeys(connection.keys(node, rawKey(pattern))));
	}

	@Override
	public K randomKey(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		return doInCluster(connection -> deserializeKey(connection.randomKey(node)));
	}

	@Override
	public String ping(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		return doInCluster(connection -> connection.ping(node));
	}

	@Override
	public void addSlots(@NonNull RedisClusterNode node, int... slots) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.clusterAddSlots(node, slots);
			return null;
		});
	}

	@Override
	public void addSlots(@NonNull RedisClusterNode node, @NonNull SlotRange range) {

		Assert.notNull(node, "ClusterNode must not be null");
		Assert.notNull(range, "Range must not be null");

		addSlots(node, range.getSlotsArray());
	}

	@Override
	public void bgReWriteAof(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.bgReWriteAof(node);
			return null;
		});
	}

	@Override
	public void bgSave(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.bgSave(node);
			return null;
		});
	}

	@Override
	public void meet(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.clusterMeet(node);
			return null;
		});
	}

	@Override
	public void forget(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.clusterForget(node);
			return null;
		});
	}

	@Override
	public void flushDb(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.flushDb(node);
			return null;
		});
	}

	@Override
	public void flushDb(@NonNull RedisClusterNode node, @NonNull FlushOption option) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.flushDb(node, option);
			return null;
		});
	}

	@Override
	public Collection<RedisClusterNode> getReplicas(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		return doInCluster(connection -> connection.clusterGetReplicas(node));
	}

	@Override
	public void save(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.save(node);
			return null;
		});
	}

	@Override
	public void shutdown(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.shutdown(node);
			return null;
		});
	}

	@Override
	public void reshard(@NonNull RedisClusterNode source, int slot, @NonNull RedisClusterNode target) {

		Assert.notNull(source, "Source node must not be null");
		Assert.notNull(target, "Target node must not be null");

		doInCluster((RedisClusterCallback<Void>) connection -> {

			connection.clusterSetSlot(target, slot, AddSlots.IMPORTING);
			connection.clusterSetSlot(source, slot, AddSlots.MIGRATING);
			List<byte[]> keys = connection.clusterGetKeysInSlot(slot, Integer.MAX_VALUE);

			for (byte[] key : keys) {
				connection.migrate(key, source, 0, MigrateOption.COPY);
			}
			connection.clusterSetSlot(target, slot, AddSlots.NODE);
			return null;
		});
	}

	/**
	 * Executed wrapped command upon {@link RedisClusterConnection}.
	 *
	 * @param callback must not be {@literal null}.
	 * @return execution result. Can be {@literal null}.
	 */
	<T> T doInCluster(@NonNull RedisClusterCallback<T> callback) {

		Assert.notNull(callback, "ClusterCallback must not be null");

		try (RedisClusterConnection connection = template.getConnectionFactory().getClusterConnection()) {
			return callback.doInRedis(connection);
		}
	}

}
