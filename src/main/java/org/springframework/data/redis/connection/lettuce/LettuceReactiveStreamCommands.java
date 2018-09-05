/*
 * Copyright 2018 the original author or authors.
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

import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * {@link ReactiveStreamCommands} implementation for {@literal Lettuce}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class LettuceReactiveStreamCommands implements ReactiveStreamCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStreamCommands}.
	 *
	 * @param connection must not be {@literal null}.
	 */
	LettuceReactiveStreamCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xAck(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<AcknowledgeCommand, Long>> xAck(Publisher<AcknowledgeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getGroup(), "Group must not be null!");
			Assert.notNull(command.getMessageIds(), "MessageIds must not be null!");

			return cmd.xack(command.getKey(), ByteUtils.getByteBuffer(command.getGroup()),
					command.getMessageIds().toArray(new String[0])).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xAdd(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<AddStreamMessage, String>> xAdd(Publisher<AddStreamMessage> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getBody(), "Body must not be null!");

			return cmd.xadd(command.getKey(), command.getBody()).map(value -> new CommandResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xDel(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<DeleteCommand, Long>> xDel(Publisher<DeleteCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getMessageIds(), "MessageIds must not be null!");

			return cmd.xdel(command.getKey(), command.getMessageIds().toArray(new String[0]))
					.map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xLen(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> xLen(Publisher<KeyCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");

			return cmd.xlen(command.getKey()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<RangeCommand, Flux<StreamMessage<ByteBuffer, ByteBuffer>>>> xRange(
			Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");
			Assert.notNull(command.getLimit(), "Limit must not be null!");

			io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(command.getRange(), Function.identity());
			io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(command.getLimit());

			return new CommandResponse<>(command, cmd.xrange(command.getKey(), lettuceRange, lettuceLimit)
					.map(StreamConverters::toStreamMessage));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#read(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<ReadCommand, Flux<StreamMessage<ByteBuffer, ByteBuffer>>>> read(
			Publisher<ReadCommand> commands) {

		return Flux.from(commands).map(command -> {

			Assert.notNull(command.getStreamOffsets(), "StreamOffsets must not be null!");
			Assert.notNull(command.getReadOptions(), "ReadOptions must not be null!");

			StreamReadOptions readOptions = command.getReadOptions();

			if (readOptions.getBlock() != null && readOptions.getBlock() > 0) {
				return new CommandResponse<>(command, connection.executeDedicated(cmd -> doRead(command, readOptions, cmd)));
			}

			return new CommandResponse<>(command, connection.execute(cmd -> doRead(command, readOptions, cmd)));
		});
	}

	private static Flux<StreamMessage<ByteBuffer, ByteBuffer>> doRead(ReadCommand command, StreamReadOptions readOptions,
			RedisClusterReactiveCommands<ByteBuffer, ByteBuffer> cmd) {

		StreamOffset<ByteBuffer>[] streamOffsets = toStreamOffsets(command.getStreamOffsets());
		XReadArgs args = StreamConverters.toReadArgs(readOptions);

		if (command.getConsumer() == null) {
			return cmd.xread(args, streamOffsets)
					.map(StreamConverters::toStreamMessage);
		}

		io.lettuce.core.Consumer<ByteBuffer> lettuceConsumer = toConsumer(command.getConsumer());

		return cmd.xreadgroup(lettuceConsumer, args, streamOffsets)
				.map(StreamConverters::toStreamMessage);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xRevRange(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<CommandResponse<RangeCommand, Flux<StreamMessage<ByteBuffer, ByteBuffer>>>> xRevRange(
			Publisher<RangeCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).map(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getRange(), "Range must not be null!");
			Assert.notNull(command.getLimit(), "Limit must not be null!");

			io.lettuce.core.Range<String> lettuceRange = RangeConverter.toRange(command.getRange(), Function.identity());
			io.lettuce.core.Limit lettuceLimit = LettuceConverters.toLimit(command.getLimit());

			return new CommandResponse<>(command, cmd.xrevrange(command.getKey(), lettuceRange, lettuceLimit)
					.map(StreamConverters::toStreamMessage));
		}));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveStreamCommands#xTrim(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyCommand, Long>> xTrim(Publisher<TrimCommand> commands) {

		return connection.execute(cmd -> Flux.from(commands).concatMap(command -> {

			Assert.notNull(command.getKey(), "Key must not be null!");
			Assert.notNull(command.getCount(), "Count must not be null!");

			return cmd.xtrim(command.getKey(), command.getCount()).map(value -> new NumericResponse<>(command, value));
		}));
	}

	@SuppressWarnings("unchecked")
	private static <T> StreamOffset<T>[] toStreamOffsets(Collection<RedisStreamCommands.StreamOffset<T>> streams) {

		return streams.stream().map(it -> StreamOffset.from(it.getKey(), it.getOffset().getOffset()))
				.toArray(StreamOffset[]::new);
	}

	private static io.lettuce.core.Consumer<ByteBuffer> toConsumer(Consumer consumer) {
		return io.lettuce.core.Consumer.from(ByteUtils.getByteBuffer(consumer.getGroup()),
				ByteUtils.getByteBuffer(consumer.getName()));
	}
}
