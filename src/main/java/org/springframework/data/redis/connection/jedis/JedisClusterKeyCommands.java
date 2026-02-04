/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.commands.JedisBinaryCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterCommandCallback;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisMultiKeyClusterCommandCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Cluster {@link RedisKeyCommands} implementation for Jedis.
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author ihaohong
 * @author Dan Smith
 * @author Yordan Tsintsov
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterKeyCommands extends JedisKeyCommands {

	private final JedisClusterConnection connection;

	JedisClusterKeyCommands(JedisClusterConnection connection) {
		super(connection);
		this.connection = connection;
	}

	@Override
	public Boolean copy(byte @NonNull [] sourceKey, byte @NonNull [] targetKey, boolean replace) {

		Assert.notNull(sourceKey, "source key must not be null");
		Assert.notNull(targetKey, "target key must not be null");

		return connection.getCluster().copy(sourceKey, targetKey, replace);
	}

	@Override
	public Long del(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.del(keys);
		}

		return (long) connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Long>) JedisBinaryCommands::del, Arrays.asList(keys))
				.resultsAsList().size();
	}

	@Override
	public Long unlink(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.unlink(keys);
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Long>) JedisBinaryCommands::unlink, Arrays.asList(keys))
				.resultsAsList().stream().mapToLong(val -> val).sum();
	}

	@Override
	public Long touch(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.touch(keys);
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Long>) JedisBinaryCommands::touch, Arrays.asList(keys))
				.resultsAsList().stream().mapToLong(val -> val).sum();
	}

	@Override
	public Set<byte @NonNull []> keys(byte @NonNull [] pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		Collection<Set<byte[]>> keysPerNode = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes((JedisClusterCommandCallback<Set<byte[]>>) client -> client.keys(pattern))
				.resultsAsList();

		Set<byte[]> keys = new HashSet<>();
		for (Set<byte[]> keySet : keysPerNode) {
			keys.addAll(keySet);
		}
		return keys;
	}

	/**
	 * Get keys matching pattern from specific cluster node.
	 *
	 * @param node must not be {@literal null}.
	 * @param pattern must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	public Set<byte @NonNull []> keys(@NonNull RedisClusterNode node, byte @NonNull [] pattern) {

		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(pattern, "Pattern must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Set<byte[]>>) client -> client.keys(pattern), node)
				.getValue();
	}

	@Override
	public Cursor<byte @NonNull []> scan(@Nullable ScanOptions options) {
		throw new InvalidDataAccessApiUsageException("Scan is not supported across multiple nodes within a cluster");
	}

	/**
	 * Use a {@link Cursor} to iterate over keys stored at the given {@link RedisClusterNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	Cursor<byte @NonNull []> scan(@NonNull RedisClusterNode node, @NonNull ScanOptions options) {

		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(options, "Options must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<Cursor<byte[]>>) client -> {

					return new ScanCursor<byte @NonNull []>(0, options) {

						@Override
						protected ScanIteration<byte @NonNull []> doScan(@NonNull CursorId cursorId, @NonNull ScanOptions options) {

							ScanParams params = JedisConverters.toScanParams(options);
							ScanResult<String> result = client.scan(cursorId.getCursorId(), params);
							return new ScanIteration<>(CursorId.of(result.getCursor()),
									JedisConverters.stringListToByteList().convert(result.getResult()));
						}
					}.open();
				}, node).getValue();
	}

	@Override
	public byte[] randomKey() {

		List<RedisClusterNode> nodes = new ArrayList<>(
				connection.getTopologyProvider().getTopology().getActiveMasterNodes());
		Set<RedisNode> inspectedNodes = new HashSet<>(nodes.size());

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

	/**
	 * Get a random key from a specific cluster node.
	 *
	 * @param node must not be {@literal null}.
	 * @return the random key or {@literal null}.
	 */
	public byte[] randomKey(@NonNull RedisClusterNode node) {

		Assert.notNull(node, "RedisClusterNode must not be null");

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((JedisClusterCommandCallback<byte[]>) JedisBinaryCommands::randomBinaryKey, node).getValue();
	}

	@Override
	public void rename(byte @NonNull [] oldKey, byte @NonNull [] newKey) {

		Assert.notNull(oldKey, "Old key must not be null");
		Assert.notNull(newKey, "New key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(oldKey, newKey)) {
			super.rename(oldKey, newKey);
			return;
		}

		byte[] value = dump(oldKey);

		if (value != null && value.length > 0) {

			restore(newKey, 0, value, true);
			del(oldKey);
		}
	}

	@Override
	public Boolean renameNX(byte @NonNull [] sourceKey, byte @NonNull [] targetKey) {

		Assert.notNull(sourceKey, "Source key must not be null");
		Assert.notNull(targetKey, "Target key must not be null");

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

	@Override
	public Boolean move(byte @NonNull [] key, int dbIndex) {
		throw new InvalidDataAccessApiUsageException("Cluster mode does not allow moving keys");
	}

	@Override
	public Long sort(byte @NonNull [] key, @Nullable SortParameters params, byte @NonNull [] storeKey) {

		Assert.notNull(key, "Key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(key, storeKey)) {
			return super.sort(key, params, storeKey);
		}

		List<byte[]> sorted = sort(key, params);
		byte[][] arr = new byte[sorted.size()][];
		connection.keyCommands().unlink(storeKey);
		connection.listCommands().lPush(storeKey, sorted.toArray(arr));
		return (long) sorted.size();
	}

	@Override
	public Long exists(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.exists(keys);
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<Boolean>) JedisBinaryCommands::exists, Arrays.asList(keys))
				.resultsAsList().stream().mapToLong(val -> ObjectUtils.nullSafeEquals(val, Boolean.TRUE) ? 1 : 0).sum();
	}
}
