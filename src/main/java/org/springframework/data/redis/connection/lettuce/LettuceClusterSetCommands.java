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

import io.lettuce.core.api.sync.RedisSetCommands;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceMultiKeyClusterCommandCallback;
import org.springframework.data.redis.connection.util.ByteArraySet;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceClusterSetCommands extends LettuceSetCommands {

	private final LettuceClusterConnection connection;

	LettuceClusterSetCommands(LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sMove(byte[], byte[], byte[])
	 */
	@Override
	public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {

		Assert.notNull(srcKey, "Source key must not be null!");
		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(srcKey, destKey)) {
			return super.sMove(srcKey, destKey, value);
		}

		if (connection.keyCommands().exists(srcKey)) {
			if (sRem(srcKey, value) > 0 && !sIsMember(destKey, value)) {
				return LettuceConverters.toBoolean(sAdd(destKey, value));
			}
		}
		return Boolean.FALSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sInter(byte[][])
	 */
	@Override
	public Set<byte[]> sInter(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sInter(keys);
		}

		Collection<Set<byte[]>> nodeResult = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((LettuceMultiKeyClusterCommandCallback<Set<byte[]>>) RedisSetCommands::smembers,
						Arrays.asList(keys))
				.resultsAsList();

		ByteArraySet result = null;
		for (Set<byte[]> entry : nodeResult) {

			ByteArraySet tmp = new ByteArraySet(entry);
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sInterStore(byte[], byte[][])
	 */
	@Override
	public Long sInterStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sUnion(byte[][])
	 */
	@Override
	public Set<byte[]> sUnion(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sUnion(keys);
		}

		Collection<Set<byte[]>> nodeResult = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((LettuceMultiKeyClusterCommandCallback<Set<byte[]>>) RedisSetCommands::smembers,
						Arrays.asList(keys))
				.resultsAsList();

		ByteArraySet result = new ByteArraySet();
		for (Set<byte[]> entry : nodeResult) {
			result.addAll(entry);
		}

		if (result.isEmpty()) {
			return Collections.emptySet();
		}

		return result.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sUnionStore(byte[], byte[][])
	 */
	@Override
	public Long sUnionStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sDiff(byte[][])
	 */
	@Override
	public Set<byte[]> sDiff(byte[]... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements!");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.sDiff(keys);
		}

		byte[] source = keys[0];
		byte[][] others = Arrays.copyOfRange(keys, 1, keys.length);

		ByteArraySet values = new ByteArraySet(sMembers(source));
		Collection<Set<byte[]>> nodeResult = connection.getClusterCommandExecutor()
				.executeMultiKeyCommand((LettuceMultiKeyClusterCommandCallback<Set<byte[]>>) RedisSetCommands::smembers,
						Arrays.asList(others))
				.resultsAsList();

		if (values.isEmpty()) {
			return Collections.emptySet();
		}

		for (Set<byte[]> toSubstract : nodeResult) {
			values.removeAll(toSubstract);
		}

		return values.asRawSet();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#sDiffStore(byte[], byte[][])
	 */
	@Override
	public Long sDiffStore(byte[] destKey, byte[]... keys) {

		Assert.notNull(destKey, "Destination key must not be null!");
		Assert.notNull(keys, "Source keys must not be null!");
		Assert.noNullElements(keys, "Source keys must not contain null elements!");

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
}
