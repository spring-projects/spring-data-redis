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

import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.connection.jedis.JedisConnection.JedisResult;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
class JedisHyperLogLogCommands implements RedisHyperLogLogCommands {

	private final JedisConnection connection;

	public JedisHyperLogLogCommands(JedisConnection connection) {
		this.connection = connection;
	}

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
				pipeline(connection.newJedisResult(connection.getPipeline().pfadd(key, values)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pfadd(key, values)));
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
		Assert.noNullElements(keys, "Keys for PFOUNT must not contain 'null'.");

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().pfcount(keys)));
				return null;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pfcount(keys)));
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

		try {
			if (isPipelined()) {
				pipeline(connection.newJedisResult(connection.getPipeline().pfmerge(destinationKey, sourceKeys)));
				return;
			}
			if (isQueueing()) {
				transaction(connection.newJedisResult(connection.getTransaction().pfmerge(destinationKey, sourceKeys)));
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
