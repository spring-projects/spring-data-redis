/*
 * Copyright 2017-2022 the original author or authors.
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.PipelineBinaryCommands;

import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class JedisHyperLogLogCommands implements RedisHyperLogLogCommands {

	private final JedisConnection connection;

	JedisHyperLogLogCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long pfAdd(byte[] key, byte[]... values) {

		Assert.notEmpty(values, "PFADD requires at least one non 'null' value.");
		Assert.noNullElements(values, "Values for PFADD must not contain 'null'.");

		return connection.invoke().just(Jedis::pfadd, PipelineBinaryCommands::pfadd, key, values);
	}

	@Override
	public Long pfCount(byte[]... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key.");
		Assert.noNullElements(keys, "Keys for PFCOUNT must not contain 'null'.");

		return connection.invoke().just(Jedis::pfcount, PipelineBinaryCommands::pfcount, keys);
	}

	@Override
	public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {

		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(sourceKeys, "Source keys must not be null");
		Assert.noNullElements(sourceKeys, "Keys for PFMERGE must not contain 'null'.");

		connection.invoke().just(Jedis::pfmerge, PipelineBinaryCommands::pfmerge, destinationKey, sourceKeys);
	}

}
