/*
 * Copyright 2016. the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterHyperLogLogCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since @since 2.0
 */
public class LettuceReactiveClusterHyperLogLogCommands extends LettuceReactiveHyperLogLogCommands
		implements ReactiveClusterHyperLogLogCommands {

	/**
	 * Create new {@link LettuceReactiveHyperLogLogCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterHyperLogLogCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<ReactiveRedisConnection.BooleanResponse<PfMergeCommand>> pfMerge(Publisher<PfMergeCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null for PFMERGE");
			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty for PFMERGE!");

			List<ByteBuffer> keys = new ArrayList<>(command.getSourceKeys());
			keys.add(command.getKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys.toArray(new ByteBuffer[keys.size()]))) {
				return super.pfMerge(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to same slot for PFMERGE in cluster mode."));
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<PfCountCommand, Long>> pfCount(
			Publisher<PfCountCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notEmpty(command.getKeys(), "Keys must be null or empty for PFCOUNT!");

			if (ClusterSlotHashUtil
					.isSameSlotForAllKeys(command.getKeys().toArray(new ByteBuffer[command.getKeys().size()]))) {
				return super.pfCount(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to same slot for PFCOUNT in cluster mode."));
		}));
	}
}
