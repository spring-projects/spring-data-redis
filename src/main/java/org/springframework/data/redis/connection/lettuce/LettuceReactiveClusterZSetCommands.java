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

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterZSetCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since @since 2.0
 */
public class LettuceReactiveClusterZSetCommands extends LettuceReactiveZSetCommands
		implements ReactiveClusterZSetCommands {

	/**
	 * Create new {@link LettuceReactiveSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterZSetCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<ZUnionStoreCommand, Long>> zUnionStore(
			Publisher<ZUnionStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty.");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getSourceKeys())) {
				return super.zUnionStore(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for ZUNIONSTORE command."));
		}));
	}

	@Override
	public Flux<ReactiveRedisConnection.NumericResponse<ZInterStoreCommand, Long>> zInterStore(
			Publisher<ZInterStoreCommand> commands) {
		return getConnection().execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty.");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getSourceKeys())) {
				return super.zInterStore(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for ZINTERSTORE command."));
		}));
	}
}
