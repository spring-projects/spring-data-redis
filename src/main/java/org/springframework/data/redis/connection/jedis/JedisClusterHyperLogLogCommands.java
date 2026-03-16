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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisHyperLogLogCommands} implementation for Jedis.
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterHyperLogLogCommands extends JedisHyperLogLogCommands {

	JedisClusterHyperLogLogCommands(@NonNull JedisClusterConnection connection) {
		super(connection);
	}

	@Override
	public Long pfCount(byte @NonNull [] @NonNull... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key");
		Assert.noNullElements(keys, "Keys for PFCOUNT must not contain 'null'");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
			return super.pfCount(keys);
		}

		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfcount in cluster mode");
	}

	@Override
	public void pfMerge(byte @NonNull [] destinationKey, byte @NonNull [] @NonNull... sourceKeys) {

		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(sourceKeys, "Source keys must not be null");
		Assert.noNullElements(sourceKeys, "Keys for PFMERGE must not contain 'null'");

		byte[][] allKeys = ByteUtils.mergeArrays(destinationKey, sourceKeys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			super.pfMerge(destinationKey, sourceKeys);
			return;
		}

		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfmerge in cluster mode");
	}
}
