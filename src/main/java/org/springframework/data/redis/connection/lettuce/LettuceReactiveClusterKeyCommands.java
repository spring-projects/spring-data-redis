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

import io.lettuce.core.RedisException;
import io.lettuce.core.api.reactive.RedisKeyReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterKeyCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceReactiveClusterKeyCommands extends LettuceReactiveKeyCommands implements ReactiveClusterKeyCommands {

	private LettuceReactiveRedisClusterConnection connection;

	/**
	 * Create new {@link LettuceReactiveClusterKeyCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveClusterKeyCommands(LettuceReactiveRedisClusterConnection connection) {

		super(connection);

		this.connection = connection;
	}

	@Override
	public Mono<List<ByteBuffer>> keys(RedisClusterNode node, ByteBuffer pattern) {

		return connection.execute(node, cmd -> {

			Assert.notNull(pattern, "Pattern must not be null!");

			return cmd.keys(pattern).collectList();
		}).next();
	}

	@Override
	public Mono<ByteBuffer> randomKey(RedisClusterNode node) {

		return connection.execute(node, RedisKeyReactiveCommands::randomkey).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#rename(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<RenameCommand>> rename(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "key must not be null.");
			Assert.notNull(command.getNewName(), "NewName must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getNewName())) {
				return super.rename(Mono.just(command));
			}

			Mono<Boolean> result = cmd.dump(command.getKey())
					.switchIfEmpty(Mono.error(new RedisSystemException("Cannot rename key that does not exist",
							new RedisException("ERR no such key."))))
					.flatMap(value -> cmd.restore(command.getNewName(), 0, value).flatMap(res -> cmd.del(command.getKey())))
					.map(LettuceConverters.longToBooleanConverter()::convert);

			return result.map(val -> new BooleanResponse<>(command, val));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#renameNX(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<RenameCommand>> renameNX(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null.");
			Assert.notNull(command.getNewName(), "NewName must not be null!");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getNewName())) {
				return super.renameNX(Mono.just(command));
			}

			Mono<Boolean> result = cmd.exists(command.getNewName()).flatMap(exists -> {

				if (exists == 1) {
					return Mono.just(Boolean.FALSE);
				}

				return cmd.dump(command.getKey())
						.switchIfEmpty(Mono.error(new RedisSystemException("Cannot rename key that does not exist",
								new RedisException("ERR no such key."))))
						.flatMap(value -> cmd.restore(command.getNewName(), 0, value).flatMap(res -> cmd.del(command.getKey())))
						.map(LettuceConverters.longToBooleanConverter()::convert);
			});

			return result.map(val -> new BooleanResponse<>(command, val));
		}));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceReactiveKeyCommands#move(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<MoveCommand>> move(Publisher<MoveCommand> commands) {
		throw new UnsupportedOperationException("MOVE not supported in CLUSTER mode!");
	}
}
