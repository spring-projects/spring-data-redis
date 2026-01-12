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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;

import java.util.List;
import java.util.Set;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceClusterCommandCallback;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanCursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@NullUnmarked
class LettuceClusterKeyCommands extends LettuceKeyCommands {

	private final LettuceClusterConnection connection;

	LettuceClusterKeyCommands(@NonNull LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
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
		throw new InvalidDataAccessApiUsageException("MOVE not supported in CLUSTER mode");
	}

	public byte @Nullable [] randomKey(@NonNull RedisClusterNode node) {

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((LettuceClusterCommandCallback<byte[]>) client -> client.randomkey(), node)
				.getValue();
	}

	public Set<byte @NonNull []> keys(@NonNull RedisClusterNode node, byte @NonNull [] pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		return LettuceConverters.toBytesSet(connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode((LettuceClusterCommandCallback<List<byte[]>>) client ->
						client.keys(LettuceConverters.toString(pattern)), node)
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
	Cursor<byte @NonNull []> scan(@NonNull RedisClusterNode node, @NonNull ScanOptions options) {

		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(options, "Options must not be null");

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

	@Override
	public Long sort(byte @NonNull [] key, @NonNull SortParameters params, byte @NonNull [] storeKey) {

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
}
