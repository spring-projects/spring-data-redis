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
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ReactiveClusterKeyCommands;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.util.Assert;

import com.lambdaworks.redis.RedisException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Observable;

/**
 * @author Christoph Strobl.
 * @since 2.0
 */
public class LettuceReactiveClusterKeyCommands extends LettuceReactiveKeyCommands
		implements ReactiveClusterKeyCommands {

	private LettuceReactiveRedisClusterConnection connection;

	/**
	 * Create new {@link LettuceReactiveKeyCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveClusterKeyCommands(LettuceReactiveRedisClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	@Override
	public Mono<List<ByteBuffer>> keys(RedisClusterNode node, ByteBuffer pattern) {

		return connection.execute(node, cmd -> {

			Assert.notNull(pattern, "Pattern must not be null!");

			Observable<List<ByteBuffer>> result = cmd.keys(pattern.array()).map(ByteBuffer::wrap).toList();
			return Flux.from(LettuceReactiveRedisConnection.<List<ByteBuffer>> monoConverter().convert(result));
		}).next();
	}

	@Override
	public Mono<ByteBuffer> randomKey(RedisClusterNode node) {

		return connection.execute(node, cmd -> {

			Observable<ByteBuffer> result = cmd.randomkey().map(ByteBuffer::wrap);
			return Flux.from(LettuceReactiveRedisConnection.<ByteBuffer> monoConverter().convert(result));
		}).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#rename(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<ReactiveRedisConnection.BooleanResponse<RenameCommand>> rename(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "key must not be null.");
			Assert.notNull(command.getNewName(), "NewName must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getNewName())) {
				return super.rename(Mono.just(command));
			}

			Observable<Boolean> result = cmd.dump(command.getKey().array())
					.switchIfEmpty(Observable.error(new RedisSystemException("Cannot rename key that does not exist",
							new RedisException("ERR no such key."))))
					.concatMap(value -> cmd.restore(command.getNewName().array(), 0, value)
							.concatMap(res -> cmd.del(command.getKey().array())))
					.map(LettuceConverters.longToBooleanConverter()::convert);

			return LettuceReactiveRedisConnection.<Boolean> monoConverter().convert(result)
					.map(val -> new ReactiveRedisConnection.BooleanResponse<>(command, val));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveKeyCommands#renameNX(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<ReactiveRedisConnection.BooleanResponse<RenameCommand>> renameNX(Publisher<RenameCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null.");
			Assert.notNull(command.getNewName(), "NewName must not be null");

			if (ClusterSlotHashUtil.isSameSlotForAllKeys(command.getKey(), command.getNewName())) {
				return super.renameNX(Mono.just(command));
			}

			Observable<Boolean> result =

					cmd.exists(command.getNewName().array()).concatMap(exists -> {

						if (exists == 1) {
							return Observable.just(Boolean.FALSE);
						}

						return cmd.dump(command.getKey().array())
								.switchIfEmpty(Observable.error(new RedisSystemException("Cannot rename key that does not exist",
										new RedisException("ERR no such key."))))
								.concatMap(value -> cmd.restore(command.getNewName().array(), 0, value)
										.concatMap(res -> cmd.del(command.getKey().array())))
								.map(LettuceConverters.longToBooleanConverter()::convert);

					});

			return LettuceReactiveRedisConnection.<Boolean> monoConverter().convert(result)
					.map(val -> new ReactiveRedisConnection.BooleanResponse<>(command, val));
		}));
	}
}
