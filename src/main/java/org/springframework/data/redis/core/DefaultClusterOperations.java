/*
 * Copyright 2015-2023 the original author or authors.
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

import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;
import org.springframework.lang.Nullable;
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
class DefaultClusterOperations<K, V> extends AbstractOperations<K, V> implements ClusterOperations<K, V> {

	private final RedisTemplate<K, V> template;

	/**
	 * Creates new {@link DefaultClusterOperations} delegating to the given {@link RedisTemplate}.
	 *
	 * @param template must not be {@literal null}.
	 */
	DefaultClusterOperations(RedisTemplate<K, V> template) {

		super(template);
		this.template = template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#keys(org.springframework.data.redis.connection.RedisNode, byte[])
	 */
	@Override
	public Set<K> keys(RedisClusterNode node, K pattern) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return doInCluster(connection -> deserializeKeys(connection.keys(node, rawKey(pattern))));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#randomKey(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	public K randomKey(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return doInCluster(connection -> deserializeKey(connection.randomKey(node)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#ping(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	public String ping(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return doInCluster(connection -> connection.ping(node));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#addSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void addSlots(RedisClusterNode node, int... slots) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.clusterAddSlots(node, slots);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#addSlots(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisClusterNode.SlotRange)
	 */
	@Override
	public void addSlots(RedisClusterNode node, SlotRange range) {

		Assert.notNull(node, "ClusterNode must not be null.");
		Assert.notNull(range, "Range must not be null.");

		addSlots(node, range.getSlotsArray());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.bgReWriteAof(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.bgSave(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#meet(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void meet(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.clusterMeet(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#forget(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void forget(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.clusterForget(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.flushDb(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ClusterOperations#flushDb(org.springframework.data.redis.connection.RedisClusterNode, org.springframework.data.redis.connection.RedisServerCommands.FlushOption)
	 */
	@Override
	public void flushDb(RedisClusterNode node, FlushOption option) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.flushDb(node, option);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#getSlaves(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Collection<RedisClusterNode> getSlaves(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return doInCluster(connection -> connection.clusterGetSlaves(node));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.save(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		doInCluster((RedisClusterCallback<Void>) connection -> {
			connection.shutdown(node);
			return null;
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ClusterOperations#reshard(org.springframework.data.redis.connection.RedisClusterNode, int, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void reshard(RedisClusterNode source, int slot, RedisClusterNode target) {

		Assert.notNull(source, "Source node must not be null.");
		Assert.notNull(target, "Target node must not be null.");

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
	@Nullable
	<T> T doInCluster(RedisClusterCallback<T> callback) {

		Assert.notNull(callback, "ClusterCallback must not be null!");

		try (RedisClusterConnection connection = template.getConnectionFactory().getClusterConnection()) {
			return callback.doInRedis(connection);
		}
	}

}
