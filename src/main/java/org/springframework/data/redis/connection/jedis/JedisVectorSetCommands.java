/*
 * Copyright 2025 the original author or authors.
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
import org.springframework.data.domain.Vector;
import org.springframework.data.redis.connection.RedisVectorSetCommands;
import org.springframework.util.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.PipelineBinaryCommands;
import redis.clients.jedis.params.VAddParams;

/**
 * {@link RedisVectorSetCommands} implementation for Jedis.
 *
 * @author Anne Lee
 * @since 3.5
 */
@NullUnmarked
class JedisVectorSetCommands implements RedisVectorSetCommands {

	private final JedisConnection jedisConnection;

	JedisVectorSetCommands(@NonNull JedisConnection jedisConnection) {
		this.jedisConnection = jedisConnection;
	}

	@Override
	public Boolean vAdd(byte @NonNull [] key, byte @NonNull [] vector, byte @NonNull [] element, VAddOptions options) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(vector, "Vector must not be null");
		Assert.notNull(element, "Element must not be null");

		if (options == null) {
			return jedisConnection.invoke()
					.just(Jedis::vaddFP32, PipelineBinaryCommands::vaddFP32, key, vector, element);
		}

		VAddParams params = JedisConverters.toVAddParams(options);

		if (options.getReduceDim() != null) {
			// With REDUCE dimension
			return jedisConnection.invoke()
					.just(Jedis::vaddFP32, PipelineBinaryCommands::vaddFP32, key, vector, element, options.getReduceDim(), params);
		}

		return jedisConnection.invoke()
				.just(Jedis::vaddFP32, PipelineBinaryCommands::vaddFP32, key, vector, element, params);
	}

	@Override
	public Boolean vAdd(byte @NonNull [] key, @NonNull Vector vector, byte @NonNull [] element,
						VAddOptions options) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(vector, "Vector must not be null");
		Assert.notNull(element, "Element must not be null");

		if (options == null) {
			return jedisConnection.invoke()
					.just(Jedis::vadd, PipelineBinaryCommands::vadd, key, vector.toFloatArray(), element);
		}

		VAddParams params = JedisConverters.toVAddParams(options);

		if (options.getReduceDim() != null) {
			// With REDUCE dimension
			return jedisConnection.invoke()
					.just(Jedis::vadd, PipelineBinaryCommands::vadd, key, vector.toFloatArray(), element, options.getReduceDim(), params);
		}

		return jedisConnection.invoke()
				.just(Jedis::vadd, PipelineBinaryCommands::vadd, key, vector.toFloatArray(), element, params);
	}

}
