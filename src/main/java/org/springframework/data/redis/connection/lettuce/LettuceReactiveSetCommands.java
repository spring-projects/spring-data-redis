/*
 * Copyright 2016 the original author or authors.
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
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveSetCommands;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public class LettuceReactiveSetCommands implements ReactiveSetCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveSetCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveSetCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sAdd(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SAddCommand, Long>> sAdd(Publisher<SAddCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValues(), "Values must not be null!");

			return cmd.sadd(command.getKey(), command.getValues().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sRem(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SRemCommand, Long>> sRem(Publisher<SRemCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValues(), "Values must not be null!");

			return cmd.srem(command.getKey(), command.getValues().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sPop(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<KeyCommand>> sPop(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.spop(command.getKey()).map(value -> new ByteBufferResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sMove(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SMoveCommand>> sMove(Publisher<SMoveCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getDestination(), "Destination key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			return cmd.smove(command.getKey(), command.getDestination(), command.getValue())
					.map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sCard(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> sCard(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.scard(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sIsMember(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<SIsMemberCommand>> sIsMember(Publisher<SIsMemberCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getValue(), "Value must not be null!");

			return cmd.sismember(command.getKey(), command.getValue()).map(value -> new BooleanResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sInter(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<SInterCommand, ByteBuffer>> sInter(Publisher<SInterCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");

			return cmd.sinter(command.getKeys().stream().toArray(ByteBuffer[]::new)).collectList()
					.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sInterStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SInterStoreCommand, Long>> sInterStore(Publisher<SInterStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");
			Assert.notNull(command.getKey(), "Destination key must not be null!");

			return cmd.sinterstore(command.getKey(), command.getKeys().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sInter(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<SUnionCommand, ByteBuffer>> sUnion(Publisher<SUnionCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");

			return cmd.sunion(command.getKeys().stream().toArray(ByteBuffer[]::new)).collectList()
					.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sInterStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SUnionStoreCommand, Long>> sUnionStore(Publisher<SUnionStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");
			Assert.notNull(command.getKey(), "Destination key must not be null!");

			return cmd.sunionstore(command.getKey(), command.getKeys().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sInter(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<SDiffCommand, ByteBuffer>> sDiff(Publisher<SDiffCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");

			return cmd.sdiff(command.getKeys().stream().toArray(ByteBuffer[]::new)).collectList()
					.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sInterStore(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<SDiffStoreCommand, Long>> sDiffStore(Publisher<SDiffStoreCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKeys(), "Keys must not be null!");
			Assert.notNull(command.getKey(), "Destination key must not be null!");

			return cmd.sdiffstore(command.getKey(), command.getKeys().stream().toArray(ByteBuffer[]::new))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sMembers(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<KeyCommand, ByteBuffer>> sMembers(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.smembers(command.getKey()).collectList().map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveSetCommands#sRandMembers(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<SRandMembersCommand, ByteBuffer>> sRandMember(
			Publisher<SRandMembersCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).flatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			boolean singleElement = !command.getCount().isPresent() || command.getCount().get().equals(1L);

			Mono<List<ByteBuffer>> result = singleElement ? cmd.srandmember(command.getKey()).map(Collections::singletonList)
					: cmd.srandmember(command.getKey(), command.getCount().get()).collectList();

			return result.map(value -> new MultiValueResponse<>(command, value));
		}));
	}

	protected LettuceReactiveRedisConnection getConnection() {
		return connection;
	}
}
