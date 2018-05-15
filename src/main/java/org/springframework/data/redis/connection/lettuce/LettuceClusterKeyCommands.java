/*
 * Copyright 2017-2018 the original author or authors.
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

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceClusterCommandCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceClusterKeyCommands extends LettuceKeyCommands {

	private final LettuceClusterConnection connection;

	LettuceClusterKeyCommands(LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
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

			RedisClusterNode node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

			while (inspectedNodes.contains(node)) {
				node = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
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
	public Set<byte[]> keys(byte[] pattern) {

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
	public void rename(byte[] sourceKey, byte[] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(targetKey, "Target key must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sourceKey, targetKey)) {
			super.rename(sourceKey, targetKey);
			return;
		}

		byte[] value = dump(sourceKey);

		if (value != null && value.length > 0) {

			restore(targetKey, 0, value);
			del(sourceKey);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#renameNX(byte[], byte[])
	 */
	@Override
	public Boolean renameNX(byte[] sourceKey, byte[] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null!");
		Assert.notNull(targetKey, "Target key must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(sourceKey, targetKey)) {
			return super.renameNX(sourceKey, targetKey);
		}

		byte[] value = dump(sourceKey);

		if (value != null && value.length > 0 && !exists(targetKey)) {

			restore(targetKey, 0, value);
			del(sourceKey);
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
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#randomKey(org.springframework.data.redis.connection.RedisClusterNode)
	*/
	@Nullable
	public byte[] randomKey(RedisClusterNode node) {

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((LettuceClusterCommandCallback<byte[]>) client -> client.randomkey(), node)
				.getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterConnection#keys(org.springframework.data.redis.connection.RedisClusterNode, byte[])
	 */
	@Nullable
	public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return LettuceConverters.toBytesSet(connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((LettuceClusterCommandCallback<List<byte[]>>) client -> client.keys(pattern), node)
				.getValue());
	}

	/**
	 * Use a {@link Cursor} to iterate over keys stored at the given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	Cursor<byte[]> scan(RedisClusterNode node, ScanOptions options) {

		Assert.notNull(node, "RedisClusterNode must not be null!");
		Assert.notNull(options, "Options must not be null!");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((LettuceClusterCommandCallback<ScanCursor<byte[]>>) client -> {

					return new LettuceScanCursor<byte[]>(options) {

						@Override
						protected LettuceScanIteration<byte[]> doScan(io.lettuce.core.ScanCursor cursor, ScanOptions options) {

							ScanArgs scanArgs = LettuceConverters.toScanArgs(options);

							KeyScanCursor<byte[]> keyScanCursor = client.scan(cursor, scanArgs);
							return new LettuceScanIteration<>(keyScanCursor, keyScanCursor.getKeys());
						}

					}.open();

				}, node).getValue();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sort(byte[], org.springframework.data.redis.connection.SortParameters, byte[])
	 */
	@Override
	public Long sort(byte[] key, SortParameters params, byte[] storeKey) {

		Assert.notNull(key, "Key must not be null!");

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
