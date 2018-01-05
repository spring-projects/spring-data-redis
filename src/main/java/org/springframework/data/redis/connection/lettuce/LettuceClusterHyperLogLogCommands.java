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

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.util.ByteUtils;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceClusterHyperLogLogCommands extends LettuceHyperLogLogCommands {

	LettuceClusterHyperLogLogCommands(LettuceClusterConnection connection) {
		super(connection);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {

			try {
				return super.pfCount(keys);
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfcount in cluster mode.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnection#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		byte[][] allKeys = ByteUtils.mergeArrays(destinationKey, sourceKeys);

		if (ClusterSlotHashUtil.isSameSlotForAllKeys(allKeys)) {
			try {
				super.pfMerge(destinationKey, sourceKeys);
				return;
			} catch (Exception ex) {
				throw convertLettuceAccessException(ex);
			}

		}
		throw new InvalidDataAccessApiUsageException("All keys must map to same slot for pfmerge in cluster mode.");
	}
}
