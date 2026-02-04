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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisMultiKeyClusterCommandCallback;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisStringCommands} implementation for Jedis.
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Xiaohu Zhang
 * @author dengliming
 * @author Marcin Grzejszczak
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterStringCommands extends JedisStringCommands {

	private final JedisClusterConnection connection;

	JedisClusterStringCommands(@NonNull JedisClusterConnection connection) {
		super(connection);
		this.connection = connection;
	}

	@Override
	public List<byte[]> mGet(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.mGet(keys);
		}

		return connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((JedisMultiKeyClusterCommandCallback<byte[]>) JedisBinaryCommands::get, Arrays.asList(keys))
				.resultsAsListSortBy(keys);
	}

	@Override
	public Boolean mSet(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			return super.mSet(tuples);
		}

		boolean result = true;
		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			if (!set(entry.getKey(), entry.getValue())) {
				result = false;
			}
		}
		return result;
	}

	@Override
	public Boolean mSetNX(@NonNull Map<byte @NonNull [], byte @NonNull []> tuples) {

		Assert.notNull(tuples, "Tuples must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(tuples.keySet().toArray(new byte[tuples.keySet().size()][]))) {
			return super.mSetNX(tuples);
		}

		boolean result = true;
		for (Map.Entry<byte[], byte[]> entry : tuples.entrySet()) {
			if (!setNX(entry.getKey(), entry.getValue()) && result) {
				result = false;
			}
		}
		return result;
	}

	@Override
	public Long bitOp(@NonNull BitOperation op, byte @NonNull [] destination, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(op, "BitOperation must not be null");
		Assert.notNull(destination, "Destination key must not be null");

		byte[][] allKeys = ByteUtils.mergeArrays(destination, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.bitOp(op, destination, keys);
		}

		throw new InvalidDataAccessApiUsageException("BITOP is only supported for same slot keys in cluster mode");
	}
}
