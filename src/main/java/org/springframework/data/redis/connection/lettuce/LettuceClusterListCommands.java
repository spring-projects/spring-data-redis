/*
 * Copyright 2017-2025 the original author or authors.
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

import io.lettuce.core.KeyValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceMultiKeyClusterCommandCallback;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@NullUnmarked
class LettuceClusterListCommands extends LettuceListCommands {

	private final LettuceClusterConnection connection;

	LettuceClusterListCommands(@NonNull LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	@Override
	public List<byte[]> bLPop(int timeout, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.bLPop(timeout, keys);
		}

		List<KeyValue<byte[], byte[]>> resultList = connection.getClusterCommandExecutor().executeMultiKeyCommand(
				(LettuceMultiKeyClusterCommandCallback<KeyValue<byte[], byte[]>>) (client, key) -> client.blpop(timeout, key),
				Arrays.asList(keys)).resultsAsList();

		for (KeyValue<byte[], byte[]> kv : resultList) {
			if (kv != null) {
				return LettuceConverters.toBytesList(kv);
			}
		}

		return Collections.emptyList();
	}

	@Override
	public List<byte[]> bRPop(int timeout, byte @NonNull [] @NonNull... keys) {

		Assert.notNull(keys, "Keys must not be null");
		Assert.noNullElements(keys, "Keys must not contain null elements");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.bRPop(timeout, keys);
		}

		List<KeyValue<byte[], byte[]>> resultList = connection.getClusterCommandExecutor().executeMultiKeyCommand(
				(LettuceMultiKeyClusterCommandCallback<KeyValue<byte[], byte[]>>) (client, key) -> client.brpop(timeout, key),
				Arrays.asList(keys)).resultsAsList();

		for (KeyValue<byte[], byte[]> kv : resultList) {
			if (kv != null) {
				return LettuceConverters.toBytesList(kv);
			}
		}

		return Collections.emptyList();
	}

	@Override
	public byte[] rPopLPush(byte @NonNull [] srcKey, byte @NonNull [] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			return super.rPopLPush(srcKey, dstKey);
		}

		byte[] val = rPop(srcKey);
		lPush(dstKey, val);
		return val;
	}

	@Override
	public byte[] bRPopLPush(int timeout, byte @NonNull [] srcKey, byte @NonNull [] dstKey) {

		Assert.notNull(srcKey, "Source key must not be null");
		Assert.notNull(dstKey, "Destination key must not be null");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, dstKey)) {
			return super.bRPopLPush(timeout, srcKey, dstKey);
		}

		List<byte[]> val = bRPop(timeout, srcKey);
		if (!CollectionUtils.isEmpty(val)) {
			lPush(dstKey, val.get(1));
			return val.get(1);
		}

		return null;
	}
}
