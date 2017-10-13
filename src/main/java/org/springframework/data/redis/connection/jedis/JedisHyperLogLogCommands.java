/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisHyperLogLogCommands implements RedisHyperLogLogCommands {

	private final @NonNull JedisConnection connection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHyperLogLogCommands#pfAdd(byte[], byte[][])
	 */
	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		Assert.notEmpty(values, "PFADD requires at least one non 'null' value.");
		Assert.noNullElements(values, "Values for PFADD must not contain 'null'.");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().pfadd(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().pfadd(key, values)));
				return null;
			}
			return connection.getJedis().pfadd(key, values);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHyperLogLogCommands#pfCount(byte[][])
	 */
	@Override
	public Long pfCount(byte[]... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key.");
		Assert.noNullElements(keys, "Keys for PFCOUNT must not contain 'null'.");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().pfcount(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().pfcount(keys)));
				return null;
			}
			return connection.getJedis().pfcount(keys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisHyperLogLogCommands#pfMerge(byte[], byte[][])
	 */
	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(sourceKeys, "Source keys must not be null");
		Assert.noNullElements(sourceKeys, "Keys for PFMERGE must not contain 'null'.");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getRequiredPipeline().pfmerge(destinationKey, sourceKeys)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getRequiredTransaction().pfmerge(destinationKey, sourceKeys)));
				return;
			}
			connection.getJedis().pfmerge(destinationKey, sourceKeys);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private void pipeline(JedisResult result) {
		connection.pipeline(result);
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void transaction(JedisResult result) {
		connection.transaction(result);
	}

	private RuntimeException convertJedisAccessException(Exception ex) {
		return connection.convertJedisAccessException(ex);
	}

}
