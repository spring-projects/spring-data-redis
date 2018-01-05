/*
 * Copyright 2016-2018 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterZSetCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveClusterZSetCommands extends LettuceReactiveZSetCommands implements ReactiveClusterZSetCommands {

	/**
	 * Create new {@link LettuceReactiveClusterZSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveClusterZSetCommands(LettuceReactiveRedisConnection connection) {
		super(connection);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveZSetCommands#zUnionStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZUnionStoreCommand, Long>> zUnionStore(Publisher<ZUnionStoreCommand> commands) {

		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty.");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getSourceKeys())) {
				return super.zUnionStore(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for ZUNIONSTORE command."));
		}));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveZSetCommands#zInterStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ZInterStoreCommand, Long>> zInterStore(Publisher<ZInterStoreCommand> commands) {
		return getConnection().execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notEmpty(command.getSourceKeys(), "Source keys must not be null or empty.");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getSourceKeys())) {
				return super.zInterStore(Mono.just(command));
			}

			return Mono
					.error(new InvalidDataAccessApiUsageException("All keys must map to the same slot for ZINTERSTORE command."));
		}));
	}
}
