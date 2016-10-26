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

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterListCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class LettuceReactiveClusterListCommands extends LettuceReactiveListCommands
		implements ReactiveClusterListCommands {

	/**
	 * Create new {@link LettuceReactiveListCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterListCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<PopResponse> bPop(Publisher<BPopCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");
			Assert.notNull(command.getDirection(), "Direction must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeys())) {
				return super.bPop(Mono.just(command));
			}

			return Mono.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for BPOP command."));
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.ByteBufferResponse<RPopLPushCommand>> rPopLPush(
			Publisher<RPopLPushCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getDestination(), "Destination key must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getDestination())) {
				return super.rPopLPush(Mono.just(command));
			}

			Flux<ByteBuffer> result = cmd.rpop(command.getKey())
					.flatMap(value -> cmd.lpush(command.getDestination(), value).map(x -> value));

			return result.map(value -> new ReactiveRedisConnection.ByteBufferResponse<>(command, value));
		}));
	}
}
