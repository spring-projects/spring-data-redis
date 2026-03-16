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

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisMultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanIteration;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.redis.util.KeyUtils;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisSetCommands} implementation for Jedis.
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Mingi Lee
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterSetCommands extends JedisSetCommands {

	private final JedisClusterConnection connection;

	JedisClusterSetCommands(JedisClusterConnection connection) {
		super(connection);
		this.connection = connection;
	}

	@Override
	public Boolean sMove(byte @NonNull [] srcKey, byte @NonNull [] destKey, byte @NonNull [] value) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(value, "Value must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, destKey)) {
			return super.sMove(srcKey, destKey, value);
		}

		if (connection.keyCommands().exists(srcKey)) {
			if (sRem(srcKey, value) > 0 && !sIsMember(destKey, value)) {
				return JedisConverters.toBoolean(sAdd(destKey, value));
			}
		}
		return Boolean.FALSE;
	}

	@Override
	public Set<byte @NonNull []> sInter(byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sInter(keys);
		}

		Collection<Set<byte[]>> resultList = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<Set<byte[]>>) (client, key) -> client.smembers(key),
						Arrays.asList(keys))
				.resultsAsList();

		ByteArraySet result = null;

		for (Set<byte[]> value : resultList) {

			ByteArraySet tmp = new ByteArraySet(value);
			if (result == null) {
				result = tmp;
			} else {
				result.retainAll(tmp);
				if (result.isEmpty()) {
					break;
				}
			}
		}

		if (result == null || result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	@Override
	public Long sInterStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull ... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sInterStore(destKey, keys);
		}

		Set<byte[]> result = sInter(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	@Override
	public Long sInterCard(byte @NonNull [] @NonNull ... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sInterCard(keys);
		}

		// For multi-slot clusters, calculate intersection cardinality by performing intersection
		Set<byte[]> result = sInter(keys);
		return (long) result.size();
	}

	@Override
	public Set<byte @NonNull []> sUnion(byte @NonNull [] @NonNull ... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sUnion(keys);
		}

		Collection<Set<byte[]>> resultList = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand(
						(JedisMultiKeyClusterCommandCallback<Set<byte[]>>) (client, key) -> client.smembers(key),
						Arrays.asList(keys))
				.resultsAsList();

		ByteArraySet result = new ByteArraySet();
		for (Set<byte[]> entry : resultList) {
			result.addAll(entry);
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	@Override
	public Long sUnionStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull ... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sUnionStore(destKey, keys);
		}

		Set<byte[]> result = sUnion(keys);
		if (result.isEmpty()) {
			return 0L;
		}
		return sAdd(destKey, result.toArray(new byte[result.size()][]));
	}

	@Override
	public Set<byte @NonNull []> sDiff(byte @NonNull [] @NonNull ... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sDiff(keys);
		}

		return KeyUtils.splitKeys(keys, (source, others) -> {

			ByteArraySet values = new ByteArraySet(sMembers(source));
			Collection<Set<byte[]>> resultList = connection.getClusterCommandExecutor()
					.executeMultiKeyCommand(
							(JedisMultiKeyClusterCommandCallback<Set<byte[]>>) (client, key) -> client.smembers(key),
							Arrays.asList(others))
					.resultsAsList();

			if (values.isEmpty()) {
				return Collections.emptySet();
			}

			for (Set<byte[]> singleNodeValue : resultList) {
				values.removeAll(singleNodeValue);
			}

			return values.asRawSet();
		});
	}

	@Override
	public Long sDiffStore(byte @NonNull [] destKey, byte @NonNull [] @NonNull ... keys) {

		Assert.notNull(destKey, "Destination key must not be null");
		Assert.notNull(keys, "Source keys must not be null");
		Assert.noNullElements(keys, "Source keys must not contain null elements");

		byte[][] allKeys = ByteUtils.mergeArrays(destKey, keys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			return super.sDiffStore(destKey, keys);
		}

		Set<byte[]> diff = sDiff(keys);
		if (diff.isEmpty()) {
			return 0L;
		}

		return sAdd(destKey, diff.toArray(new byte[diff.size()][]));
	}

	@Override
	public Cursor<byte @NonNull []> sScan(byte @NonNull [] key, @NonNull ScanOptions options) {

		Assert.notNull(key, "Key must not be null");

		return new ScanCursor<byte[]>(options) {

			@Override
			protected ScanIteration<byte[]> doScan(CursorId cursorId, ScanOptions options) {

				ScanParams params = JedisConverters.toScanParams(options);
				ScanResult<byte[]> result = getConnection().getJedis().sscan(key, JedisConverters.toBytes(cursorId), params);
				return new ScanIteration<>(CursorId.of(result.getCursor()), result.getResult());
			}
		}.open();
	}
}
