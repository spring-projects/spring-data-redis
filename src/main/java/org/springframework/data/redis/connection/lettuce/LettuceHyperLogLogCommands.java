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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.api.async.RedisHLLAsyncCommands;
import io.lettuce.core.api.sync.RedisHLLCommands;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.connection.lettuce.LettuceResult.LettuceTxResult;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class LettuceHyperLogLogCommands implements RedisHyperLogLogCommands {

	private final @NonNull LettuceConnection connection;

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
				RedisHLLAsyncCommands<byte[], byte[]> asyncConnection = getAsyncConnection();
				pipeline(connection.newLettuceResult(asyncConnection.pfadd(key, values)));
				return null;
			}

			if (isQueueing()) {
				RedisHLLAsyncCommands<byte[], byte[]> asyncConnection = getAsyncConnection();
				transaction(connection.newLettuceTxResult(asyncConnection.pfadd(key, values)));
				return null;
			}

			RedisHLLCommands<byte[], byte[]> connection = getConnection();
			return connection.pfadd(key, values);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				RedisHLLAsyncCommands<byte[], byte[]> asyncConnection = getAsyncConnection();
				pipeline(connection.newLettuceResult(asyncConnection.pfcount(keys)));
				return null;
			}

			if (isQueueing()) {
				RedisHLLAsyncCommands<byte[], byte[]> asyncConnection = getAsyncConnection();
				transaction(connection.newLettuceTxResult(asyncConnection.pfcount(keys)));
				return null;
			}

			RedisHLLCommands<byte[], byte[]> connection = getConnection();
			return connection.pfcount(keys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
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
				RedisHLLAsyncCommands<byte[], byte[]> asyncConnection = getAsyncConnection();
				pipeline(connection.newLettuceResult(asyncConnection.pfmerge(destinationKey, sourceKeys)));
				return;
			}

			if (isQueueing()) {
				RedisHLLAsyncCommands<byte[], byte[]> asyncConnection = getAsyncConnection();
				transaction(connection.newLettuceTxResult(asyncConnection.pfmerge(destinationKey, sourceKeys)));
				return;
			}

			RedisHLLCommands<byte[], byte[]> connection = getConnection();
			connection.pfmerge(destinationKey, sourceKeys);
		} catch (Exception ex) {
			throw convertLettuceAccessException(ex);
		}
	}

	private boolean isPipelined() {
		return connection.isPipelined();
	}

	private boolean isQueueing() {
		return connection.isQueueing();
	}

	private void pipeline(LettuceResult result) {
		connection.pipeline(result);
	}

	private void transaction(LettuceTxResult result) {
		connection.transaction(result);
	}

	private RedisClusterAsyncCommands<byte[], byte[]> getAsyncConnection() {
		return connection.getAsyncConnection();
	}

	public RedisClusterCommands<byte[], byte[]> getConnection() {
		return connection.getConnection();
	}

	protected DataAccessException convertLettuceAccessException(Exception ex) {
		return connection.convertLettuceAccessException(ex);
	}
}
