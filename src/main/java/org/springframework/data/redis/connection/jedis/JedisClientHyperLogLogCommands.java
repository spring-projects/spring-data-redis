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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.util.Assert;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientHyperLogLogCommands implements RedisHyperLogLogCommands {

	private final JedisClientConnection connection;

	JedisClientHyperLogLogCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long pfAdd(byte @NonNull [] key, byte @NonNull [] @NonNull... values) {

		Assert.notEmpty(values, "PFADD requires at least one non 'null' value");
		Assert.noNullElements(values, "Values for PFADD must not contain 'null'");

		return connection.execute(client -> client.pfadd(key, values), pipeline -> pipeline.pfadd(key, values));
	}

	@Override
	public Long pfCount(byte @NonNull [] @NonNull... keys) {

		Assert.notEmpty(keys, "PFCOUNT requires at least one non 'null' key");
		Assert.noNullElements(keys, "Keys for PFCOUNT must not contain 'null'");

		return connection.execute(client -> client.pfcount(keys), pipeline -> pipeline.pfcount(keys));
	}

	@Override
	public void pfMerge(byte @NonNull [] destinationKey, byte @NonNull [] @NonNull... sourceKeys) {

		Assert.notNull(destinationKey, "Destination key must not be null");
		Assert.notNull(sourceKeys, "Source keys must not be null");
		Assert.noNullElements(sourceKeys, "Keys for PFMERGE must not contain 'null'");

		connection.execute(client -> client.pfmerge(destinationKey, sourceKeys),
				pipeline -> pipeline.pfmerge(destinationKey, sourceKeys));
	}

}
