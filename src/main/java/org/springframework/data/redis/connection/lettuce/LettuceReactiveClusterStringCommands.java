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
import org.springframework.data.redis.connection.ReactiveClusterStringCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveClusterStringCommands extends LettuceReactiveStringCommands
		implements ReactiveClusterStringCommands {

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterStringCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<BitOpCommand, Long>> bitOp(Publisher<BitOpCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			List<ByteBuffer> keys = new ArrayList<>(command.getKeys());
			keys.add(command.getDestinationKey());

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(keys)) {
				return super.bitOp(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for BITOP command."));
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.BooleanResponse<MSetCommand>> mSetNX(Publisher<MSetCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKeyValuePairs().keySet())) {
				return super.mSetNX(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for MSETNX command."));
		}));
	}
}
