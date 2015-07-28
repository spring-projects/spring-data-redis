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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterCommands.AddSlots;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterNode.SlotRange;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;
import org.springframework.util.Assert;

/**
 * Default {@link ClusterOperations} implementation.
 * 
 * @author Christoph Strobl
 * @since 1.7
 * @param <K>
 * @param <V>
 */
public class DefaultClusterOperations<K, V> extends AbstractOperations<K, V> implements ClusterOperations<K, V> {

	private final RedisTemplate<K, V> template;

	/**
	 * Creates new {@link DefaultClusterOperations} delegating to the given {@link RedisTemplate}.
	 * 
	 * @param template must not be {@literal null}.
	 */
	public DefaultClusterOperations(RedisTemplate<K, V> template) {

		super(template);
		this.template = template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#keys(org.springframework.data.redis.connection.RedisNode, byte[])
	 */
	@Override
	public Set<K> keys(final RedisClusterNode node, final K pattern) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return execute(new RedisClusterCallback<Set<K>>() {

			@Override
			public Set<K> doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return deserializeKeys(connection.keys(node, rawKey(pattern)));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#randomKey(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	public K randomKey(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return execute(new RedisClusterCallback<K>() {

			@Override
			public K doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return deserializeKey(connection.randomKey(node));
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#ping(org.springframework.data.redis.connection.RedisNode)
	 */
	@Override
	public String ping(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return execute(new RedisClusterCallback<String>() {

			@Override
			public String doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return connection.ping(node);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#addSlots(org.springframework.data.redis.connection.RedisClusterNode, int[])
	 */
	@Override
	public void addSlots(final RedisClusterNode node, final int... slots) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.clusterAddSlots(node, slots);
				return null;
			}
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
	public void bgReWriteAof(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.bgReWriteAof(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.bgSave(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#meet(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void meet(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.clusterMeet(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#forget(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void forget(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.clusterForget(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.flushDb(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#getSlaves(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Collection<RedisClusterNode> getSlaves(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		return execute(new RedisClusterCallback<Collection<RedisClusterNode>>() {

			@Override
			public Collection<RedisClusterNode> doInRedis(RedisClusterConnection connection) throws DataAccessException {
				return connection.clusterGetSlaves(node);
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.save(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.RedisClusterOperations#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(final RedisClusterNode node) {

		Assert.notNull(node, "ClusterNode must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {
				connection.shutdown(node);
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ClusterOperations#reshard(org.springframework.data.redis.connection.RedisClusterNode, int, org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void reshard(final RedisClusterNode source, final int slot, final RedisClusterNode target) {

		Assert.notNull(source, "Source node must not be null.");
		Assert.notNull(target, "Target node must not be null.");

		execute(new RedisClusterCallback<Void>() {

			@Override
			public Void doInRedis(RedisClusterConnection connection) throws DataAccessException {

				connection.clusterSetSlot(target, slot, AddSlots.IMPORTING);
				connection.clusterSetSlot(source, slot, AddSlots.MIGRATING);
				List<byte[]> keys = connection.clusterGetKeysInSlot(slot, Integer.MAX_VALUE);

				for (byte[] key : keys) {
					connection.migrate(key, source, 0, MigrateOption.COPY);
				}
				connection.clusterSetSlot(target, slot, AddSlots.NODE);
				return null;
			}
		});
	}

	/**
	 * Executed wrapped command upon {@link RedisClusterConnection}.
	 * 
	 * @param callback
	 * @return
	 */
	public <T> T execute(RedisClusterCallback<T> callback) {

		Assert.notNull(callback, "ClusterCallback must not be null!");

		RedisClusterConnection connection = template.getConnectionFactory().getClusterConnection();

		try {
			return callback.doInRedis(connection);
		} finally {
			connection.close();
		}
	}
}
