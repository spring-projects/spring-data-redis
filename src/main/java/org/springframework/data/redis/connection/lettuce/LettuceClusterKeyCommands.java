/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceClusterCommandCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class LettuceClusterKeyCommands extends LettuceKeyCommands {

	private final LettuceClusterConnection connection;

	public LettuceClusterKeyCommands(LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#scan(long, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<byte[]> scan(long cursorId, ScanOptions options) {
		throw new InvalidDataAccessApiUsageException("Scan is not supported accros multiple nodes within a cluster.");
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.lettuce.LettuceConnection#randomKey()
	*/
	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = connection.clusterGetNodes();
		Set<RedisClusterNode> inspectedNodes = new HashSet<>(nodes.size());

		do {

			RedisClusterNode node = nodes.get(new Random().nextInt(nodes.size()));

			while (inspectedNodes.contains(node)) {
				node = nodes.get(new Random().nextInt(nodes.size()));
			}
			inspectedNodes.add(node);
			byte[] key = randomKey(node);

			if (key != null && key.length > 0) {
				return key;
			}
		} while (nodes.size() != inspectedNodes.size());

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#keys(byte[])
	 */
	@Override
	public Set<byte[]> keys(final byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		Collection<List<byte[]>> keysPerNode = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((LettuceClusterCommandCallback<List<byte[]>>) connection -> connection.keys(pattern))
				.resultsAsList();

		Set<byte[]> keys = new HashSet<>();

		for (List<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#rename(byte[], byte[])
	 */
	@Override
	public void rename(byte[] oldName, byte[] newName) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldName, newName)) {
			super.rename(oldName, newName);
			return;
		}

		byte[] value = dump(oldName);

		if (value != null && value.length > 0) {

			restore(newName, 0, value);
			del(oldName);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(byte[] oldName, byte[] newName) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldName, newName)) {
			return super.renameNX(oldName, newName);
		}

		byte[] value = dump(oldName);

		if (value != null && value.length > 0 && !exists(newName)) {

			restore(newName, 0, value);
			del(oldName);
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.lettuce.LettuceConnection#move(byte[], int)
	*/
	@Override
	public Boolean move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("MOVE not supported in CLUSTER mode!");
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.lettuce.LettuceConnection#del(byte[][])
	*/
	@Override
	public Long del(byte[]... keys) {

		Assert.noNullElements(keys, "Keys must not be null or contain null key!");

		// Routing for mget is handled by lettuce.
		return super.del(keys);
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.redis.connection.RedisClusterConnection#randomKey(org.springframework.data.redis.connection.RedisClusterNode)
	*/
	public byte[] randomKey(RedisClusterNode node) {

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(new LettuceClusterCommandCallback<byte[]>() {

					@Override
					public byte[] doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.randomkey();
					}
				}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#keys(org.springframework.data.redis.connection.RedisClusterNode, byte[])
	 */
	public Set<byte[]> keys(RedisClusterNode node, final byte[] pattern) {

		return LettuceConverters.toBytesSet(connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.keys(pattern);
					}
				}, node).getValue());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(key, storeKey)) {
			return super.sort(key, params, storeKey);
		}

		List<byte[]> sorted = sort(key, params);
		if (!CollectionUtils.isEmpty(sorted)) {

			byte[][] arr = new byte[sorted.size()][];
			switch (type(key)) {

				case SET:
					connection.setCommands().sAdd(storeKey, sorted.toArray(arr));
					return 1L;
				case LIST:
					connection.listCommands().lPush(storeKey, sorted.toArray(arr));
					return 1L;
				default:
					throw new IllegalArgumentException("sort and store is only supported for SET and LIST");
			}
		}
		return 0L;
	}

}
