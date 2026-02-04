/*
 * Copyright 2026-present the original author or authors.
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

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientClusterHyperLogLogCommands implements RedisHyperLogLogCommands {

	private final JedisClientClusterConnection connection;

	JedisClientClusterHyperLogLogCommands(JedisClientClusterConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		Assert.notEmpty(values, "PFADD requires at least one non 'null' value");
		Assert.noNullElements(values, "Values for PFADD must not contain 'null'");

		try {
			return connection.getClusterClient().pfadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public Long pfCount(byte[]... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key");
		Assert.noNullElements(keys, "Keys for PFCOUNT must not contain 'null'");

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {

			try {
				return connection.getClusterClient().pfcount(keys);
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfcount in cluster mode");
	}

	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(sourceKeys, "Source keys must not be null");
		Assert.noNullElements(sourceKeys, "Keys for PFMERGE must not contain 'null'");

		byte[][] allKeys = ByteUtils.mergeArrays(destinationKey, sourceKeys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				connection.getClusterClient().pfmerge(destinationKey, sourceKeys);
				return;
			} catch (Exception ex) {
				throw convertJedisAccessException(ex);
			}
		}

		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfmerge in cluster mode");
	}

	private DataAccessException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}
}
