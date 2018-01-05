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
package org.springframework.data.redis.connection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Redis List commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveListCommands {

	/**
	 * @author Christoph Strobl
	 */
	enum Direction {
		LEFT, RIGHT
	}

	/**
	 * {@code LPUSH}/{@literal RPUSH} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	class PushCommand extends KeyCommand {

		private List<ByteBuffer> values;
		private boolean upsert;
		private Direction direction;

		private PushCommand(@Nullable ByteBuffer key, List<ByteBuffer> values, Direction direction, boolean upsert) {

			super(key);

			this.values = values;
			this.upsert = upsert;
			this.direction = direction;
		}

		/**
		 * Creates a new {@link PushCommand} for right push ({@literal RPUSH}).
		 *
		 * @return a new {@link PushCommand} for right push ({@literal RPUSH}).
		 */
		public static PushCommand right() {
			return new PushCommand(null, Collections.emptyList(), Direction.RIGHT, true);
		}

		/**
		 * Creates a new {@link PushCommand} for left push ({@literal LPUSH}).
		 *
		 * @return a new {@link PushCommand} for left push ({@literal LPUSH}).
		 */
		public static PushCommand left() {
			return new PushCommand(null, Collections.emptyList(), Direction.LEFT, true);
		}

		/**
		 * Applies the {@literal value}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link PushCommand} with {@literal value} applied.
		 */
		public PushCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return new PushCommand(null, Collections.singletonList(value), direction, upsert);
		}

		/**
		 * Applies a {@link List} of {@literal values}.
		 *
		 * @param values must not be {@literal null}.
		 * @return a new {@link PushCommand} with {@literal values} applied.
		 */
		public PushCommand values(List<ByteBuffer> values) {

			Assert.notNull(values, "Values must not be null!");

			return new PushCommand(null, new ArrayList<>(values), direction, upsert);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link PushCommand} with {@literal key} applied.
		 */
		public PushCommand to(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new PushCommand(key, values, direction, upsert);
		}

		/**
		 * Disable upsert. Constructs a new command instance with all previously configured properties.
		 *
		 * @return a new {@link PushCommand} with upsert disabled.
		 */
		public PushCommand ifExists() {
			return new PushCommand(getKey(), values, direction, false);
		}

		/**
		 * @return never {@literal null}.
		 */
		public List<ByteBuffer> getValues() {
			return values;
		}

		/**
		 * @return
		 */
		public boolean getUpsert() {
			return upsert;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Append {@literal values} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	default Mono<Long> rPush(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return push(Mono.just(PushCommand.right().values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Append {@literal values} to {@literal key} only if {@literal key} already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpushx">Redis Documentation: RPUSHX</a>
	 */
	default Mono<Long> rPushX(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return push(Mono.just(PushCommand.right().value(value).to(key).ifExists())).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@literal values} to {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 */
	default Mono<Long> lPush(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return push(Mono.just(PushCommand.left().values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@literal value} to {@literal key} if {@literal key} already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lpushx">Redis Documentation: LPUSHX</a>
	 */
	default Mono<Long> lPushX(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return push(Mono.just(PushCommand.left().value(value).to(key).ifExists())).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@link PushCommand#getValues()} to {@link PushCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lpush">Redis Documentation: LPUSH</a>
	 * @see <a href="http://redis.io/commands/rpush">Redis Documentation: RPUSH</a>
	 */
	Flux<NumericResponse<PushCommand, Long>> push(Publisher<PushCommand> commands);

	/**
	 * Get the size of list stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 */
	default Mono<Long> lLen(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return lLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of list stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/llen">Redis Documentation: LLEN</a>
	 */
	Flux<NumericResponse<KeyCommand, Long>> lLen(Publisher<KeyCommand> commands);

	/**
	 * Get elements between {@literal start} and {@literal end} from list at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 */
	default Flux<ByteBuffer> lRange(ByteBuffer key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return lRange(Mono.just(RangeCommand.key(key).fromIndex(start).toIndex(end))).flatMap(CommandResponse::getOutput);
	}

	/**
	 * Get elements in {@link RangeCommand#getRange()} from list at {@link RangeCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lrange">Redis Documentation: LRANGE</a>
	 */
	Flux<CommandResponse<RangeCommand, Flux<ByteBuffer>>> lRange(Publisher<RangeCommand> commands);

	/**
	 * Trim list at {@literal key} to elements between {@literal start} and {@literal end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 * @see <a href="http://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 */
	default Mono<Boolean> lTrim(ByteBuffer key, long start, long end) {

		Assert.notNull(key, "Key must not be null!");

		return lTrim(Mono.just(RangeCommand.key(key).fromIndex(start).toIndex(end))) //
				.next() //
				.map(BooleanResponse::getOutput);
	}

	/**
	 * Trim list at {@link RangeCommand#getKey()} to elements within {@link RangeCommand#getRange()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/ltrim">Redis Documentation: LTRIM</a>
	 */
	Flux<BooleanResponse<RangeCommand>> lTrim(Publisher<RangeCommand> commands);

	/**
	 * {@code LINDEX} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	class LIndexCommand extends KeyCommand {

		private final Long index;

		private LIndexCommand(@Nullable ByteBuffer key, Long index) {

			super(key);
			this.index = index;
		}

		/**
		 * Creates a new {@link LIndexCommand} given an {@literal index}.
		 *
		 * @param index
		 * @return a new {@link LIndexCommand} for {@literal index}.
		 */
		public static LIndexCommand elementAt(long index) {
			return new LIndexCommand(null, index);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link LIndexCommand} with {@literal key} applied.
		 */
		public LIndexCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new LIndexCommand(key, index);
		}

		/**
		 * @return
		 */
		public Long getIndex() {
			return index;
		}
	}

	/**
	 * Get element at {@literal index} form list at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return
	 * @see <a href="http://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	default Mono<ByteBuffer> lIndex(ByteBuffer key, long index) {

		Assert.notNull(key, "Key must not be null!");

		return lIndex(Mono.just(LIndexCommand.elementAt(index).from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Get element at {@link LIndexCommand#getIndex()} form list at {@link LIndexCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lindex">Redis Documentation: LINDEX</a>
	 */
	Flux<ByteBufferResponse<LIndexCommand>> lIndex(Publisher<LIndexCommand> commands);

	/**
	 * {@code LINSERT} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 */
	class LInsertCommand extends KeyCommand {

		private final @Nullable Position position;
		private final @Nullable ByteBuffer pivot;
		private final ByteBuffer value;

		private LInsertCommand(@Nullable ByteBuffer key, @Nullable Position position, @Nullable ByteBuffer pivot,
				ByteBuffer value) {

			super(key);

			this.position = position;
			this.pivot = pivot;
			this.value = value;
		}

		/**
		 * Creates a new {@link LInsertCommand} given a {@link ByteBuffer value}.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link LInsertCommand} for {@link ByteBuffer value}.
		 */
		public static LInsertCommand value(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return new LInsertCommand(null, null, null, value);
		}

		/**
		 * Applies the before {@literal pivot}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param pivot must not be {@literal null}.
		 * @return a new {@link LInsertCommand} with {@literal pivot} applied.
		 */
		public LInsertCommand before(ByteBuffer pivot) {

			Assert.notNull(pivot, "Before pivot must not be null!");

			return new LInsertCommand(getKey(), Position.BEFORE, pivot, value);
		}

		/**
		 * Applies the after {@literal pivot}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param pivot must not be {@literal null}.
		 * @return a new {@link LInsertCommand} with {@literal pivot} applied.
		 */
		public LInsertCommand after(ByteBuffer pivot) {

			Assert.notNull(pivot, "After pivot must not be null!");

			return new LInsertCommand(getKey(), Position.AFTER, pivot, value);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link LInsertCommand} with {@literal key} applied.
		 */
		public LInsertCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new LInsertCommand(key, position, pivot, value);
		}

		/**
		 * @return never {@literal null}.
		 */
		public ByteBuffer getValue() {
			return value;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Position getPosition() {
			return position;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getPivot() {
			return pivot;
		}
	}

	/**
	 * Insert {@literal value} {@link Position#BEFORE} or {@link Position#AFTER} existing {@literal pivot} for
	 * {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param position must not be {@literal null}.
	 * @param pivot must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 */
	default Mono<Long> lInsert(ByteBuffer key, Position position, ByteBuffer pivot, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(position, "Position must not be null!");
		Assert.notNull(pivot, "Pivot must not be null!");
		Assert.notNull(value, "Value must not be null!");

		LInsertCommand command = LInsertCommand.value(value);
		command = Position.BEFORE.equals(position) ? command.before(pivot) : command.after(pivot);
		command = command.forKey(key);
		return lInsert(Mono.just(command)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Insert {@link LInsertCommand#getValue()} {@link Position#BEFORE} or {@link Position#AFTER} existing
	 * {@link LInsertCommand#getPivot()} for {@link LInsertCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/linsert">Redis Documentation: LINSERT</a>
	 */
	Flux<NumericResponse<LInsertCommand, Long>> lInsert(Publisher<LInsertCommand> commands);

	/**
	 * {@code LSET} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	class LSetCommand extends KeyCommand {

		private final Long index;
		private final @Nullable ByteBuffer value;

		private LSetCommand(@Nullable ByteBuffer key, Long index, @Nullable ByteBuffer value) {

			super(key);
			this.index = index;
			this.value = value;
		}

		/**
		 * Creates a new {@link LSetCommand} given an {@literal index}.
		 *
		 * @param index
		 * @return a new {@link LSetCommand} for {@literal index}.
		 */
		public static LSetCommand elementAt(long index) {
			return new LSetCommand(null, index, null);
		}

		/**
		 * Applies the {@literal value}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link LSetCommand} with {@literal value} applied.
		 */
		public LSetCommand to(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return new LSetCommand(getKey(), index, value);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link LSetCommand} with {@literal value} applied.
		 */
		public LSetCommand forKey(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new LSetCommand(key, index, value);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getValue() {
			return value;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Long getIndex() {
			return index;
		}
	}

	/**
	 * Set the {@literal value} list element at {@literal index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	default Mono<Boolean> lSet(ByteBuffer key, long index, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return lSet(Mono.just(LSetCommand.elementAt(index).to(value).forKey(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@link LSetCommand#getValue()} list element at {@link LSetCommand#getKey()}.
	 *
	 * @param commands
	 * @return
	 * @see <a href="http://redis.io/commands/lset">Redis Documentation: LSET</a>
	 */
	Flux<BooleanResponse<LSetCommand>> lSet(Publisher<LSetCommand> commands);

	/**
	 * {@code LREM} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	class LRemCommand extends KeyCommand {

		private final Long count;
		private final @Nullable ByteBuffer value;

		private LRemCommand(@Nullable ByteBuffer key, Long count, @Nullable ByteBuffer value) {

			super(key);

			this.count = count;
			this.value = value;
		}

		/**
		 * Creates a new {@link LRemCommand} to delete all values.
		 *
		 * @return a new {@link LRemCommand} for {@link ByteBuffer value}.
		 */
		public static LRemCommand all() {
			return new LRemCommand(null, 0L, null);
		}

		/**
		 * Creates a new {@link LRemCommand} to first {@literal count} values.
		 *
		 * @return a new {@link LRemCommand} to delete first {@literal count} values.
		 */
		public static LRemCommand first(long count) {
			return new LRemCommand(null, count, null);
		}

		/**
		 * Creates a new {@link LRemCommand} to last {@literal count} values.
		 *
		 * @return a new {@link LRemCommand} to delete last {@literal count} values.
		 */
		public static LRemCommand last(long count) {

			Long value = count < 0L ? count : Math.negateExact(count);
			return new LRemCommand(null, value, null);
		}

		/**
		 * Applies the {@literal value}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param value must not be {@literal null}.
		 * @return a new {@link LRemCommand} with {@literal value} applied.
		 */
		public LRemCommand occurrencesOf(ByteBuffer value) {

			Assert.notNull(value, "Value must not be null!");

			return new LRemCommand(getKey(), count, value);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link LRemCommand} with {@literal key} applied.
		 */
		public LRemCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new LRemCommand(key, count, value);
		}

		/**
		 * @return never {@literal null}.
		 */
		public Long getCount() {
			return count;
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Removes all occurrences of {@literal value} from the list stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	default Mono<Long> lRem(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return lRem(Mono.just(LRemCommand.all().occurrencesOf(value).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Removes the first {@literal count} occurrences of {@literal value} from the list stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	default Mono<Long> lRem(ByteBuffer key, Long count, ByteBuffer value) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(count, "Count must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return lRem(Mono.just(LRemCommand.first(count).occurrencesOf(value).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Removes the {@link LRemCommand#getCount()} occurrences of {@link LRemCommand#getValue()} from the list stored at
	 * {@link LRemCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lrem">Redis Documentation: LREM</a>
	 */
	Flux<NumericResponse<LRemCommand, Long>> lRem(Publisher<LRemCommand> commands);

	/**
	 * {@code LPOP}/{@literal RPOP} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 * @see <a href="http://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	class PopCommand extends KeyCommand {

		private final Direction direction;

		private PopCommand(@Nullable ByteBuffer key, Direction direction) {

			super(key);

			this.direction = direction;
		}

		/**
		 * Creates a new {@link PopCommand} for right push ({@literal RPOP}).
		 *
		 * @return a new {@link PopCommand} for right push ({@literal RPOP}).
		 */
		public static PopCommand right() {
			return new PopCommand(null, Direction.RIGHT);
		}

		/**
		 * Creates a new {@link PopCommand} for right push ({@literal LPOP}).
		 *
		 * @return a new {@link PopCommand} for right push ({@literal LPOP}).
		 */
		public static PopCommand left() {
			return new PopCommand(null, Direction.LEFT);
		}

		/**
		 * Applies the {@literal key}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link LSetCommand} with {@literal value} applied.
		 */
		public PopCommand from(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new PopCommand(key, direction);
		}

		/**
		 * @return never {@literal null}.
		 */
		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Removes and returns first element in list stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 */
	default Mono<ByteBuffer> lPop(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return pop(Mono.just(PopCommand.left().from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Removes and returns last element in list stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	default Mono<ByteBuffer> rPop(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return pop(Mono.just(PopCommand.right().from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Removes and returns last element in list stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/lpop">Redis Documentation: LPOP</a>
	 * @see <a href="http://redis.io/commands/rpop">Redis Documentation: RPOP</a>
	 */
	Flux<ByteBufferResponse<PopCommand>> pop(Publisher<PopCommand> commands);

	/**
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 * @see <a href="http://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	class BPopCommand implements Command {

		private final List<ByteBuffer> keys;
		private final Duration timeout;
		private final Direction direction;

		private BPopCommand(List<ByteBuffer> keys, Duration timeout, Direction direction) {

			this.keys = keys;
			this.timeout = timeout;
			this.direction = direction;
		}

		/**
		 * Creates a new {@link BPopCommand} for right push ({@literal BRPOP}).
		 *
		 * @return a new {@link BPopCommand} for right push ({@literal BRPOP}).
		 */
		public static BPopCommand right() {
			return new BPopCommand(Collections.emptyList(), Duration.ZERO, Direction.RIGHT);
		}

		/**
		 * Creates a new {@link BPopCommand} for right push ({@literal BLPOP}).
		 *
		 * @return a new {@link BPopCommand} for right push ({@literal BLPOP}).
		 */
		public static BPopCommand left() {
			return new BPopCommand(Collections.emptyList(), Duration.ZERO, Direction.LEFT);
		}

		/**
		 * Applies the {@literal value}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param keys must not be {@literal null}.
		 * @return a new {@link BPopCommand} with {@literal value} applied.
		 */
		public BPopCommand from(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");

			return new BPopCommand(new ArrayList<>(keys), Duration.ZERO, direction);
		}

		/**
		 * Applies a {@link Duration timeout}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param timeout must not be {@literal null}.
		 * @return a new {@link BPopCommand} with {@link Duration timeout} applied.
		 */
		public BPopCommand blockingFor(Duration timeout) {

			Assert.notNull(timeout, "Timeout must not be null!");

			return new BPopCommand(keys, timeout, direction);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.Command#getKey()
		 */
		@Override
		public ByteBuffer getKey() {
			return null;
		}

		/**
		 * @return
		 */
		public List<ByteBuffer> getKeys() {
			return keys;
		}

		/**
		 * @return
		 */
		public Duration getTimeout() {
			return timeout;
		}

		/**
		 * @return
		 */
		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Result for {@link PopCommand}/{@link BPopCommand}.
	 *
	 * @author Christoph Strobl
	 */
	class PopResult {

		private final List<ByteBuffer> result;

		public PopResult(List<ByteBuffer> result) {
			this.result = result;
		}

		public ByteBuffer getKey() {
			return ObjectUtils.isEmpty(result) ? null : result.get(0);
		}

		public ByteBuffer getValue() {
			return ObjectUtils.isEmpty(result) ? null : result.get(1);
		}

		public List<ByteBuffer> getRaw() {
			return Collections.unmodifiableList(result);
		}
	}

	/**
	 * Result for {@link PopCommand}/{@link BPopCommand}.
	 *
	 * @author Christoph Strobl
	 */
	class PopResponse extends CommandResponse<BPopCommand, PopResult> {
		public PopResponse(BPopCommand input, PopResult output) {
			super(input, output);
		}
	}

	/**
	 * Removes and returns first element from lists stored at {@literal keys}. <br>
	 * <b>Blocks connection</b> until element available or {@literal timeout} reached.
	 *
	 * @param keys must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 */
	default Mono<PopResult> blPop(List<ByteBuffer> keys, Duration timeout) {

		Assert.notNull(keys, "Keys must not be null.");
		Assert.notNull(timeout, "Timeout must not be null.");

		return bPop(Mono.just(BPopCommand.left().from(keys).blockingFor(timeout))).next().map(PopResponse::getOutput);
	}

	/**
	 * Removes and returns last element from lists stored at {@literal keys}. <br>
	 * <b>Blocks connection</b> until element available or {@literal timeout} reached.
	 *
	 * @param keys must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	default Mono<PopResult> brPop(List<ByteBuffer> keys, Duration timeout) {

		Assert.notNull(keys, "Keys must not be null.");
		Assert.notNull(timeout, "Timeout must not be null.");

		return bPop(Mono.just(BPopCommand.right().from(keys).blockingFor(timeout))).next().map(PopResponse::getOutput);
	}

	/**
	 * Removes and returns the top {@link BPopCommand#getDirection()} element from lists stored at
	 * {@link BPopCommand#getKeys()}.<br>
	 * <b>Blocks connection</b> until element available or {@link BPopCommand#getTimeout()} reached.
	 *
	 * @param commands
	 * @return
	 * @see <a href="http://redis.io/commands/blpop">Redis Documentation: BLPOP</a>
	 * @see <a href="http://redis.io/commands/brpop">Redis Documentation: BRPOP</a>
	 */
	Flux<PopResponse> bPop(Publisher<BPopCommand> commands);

	/**
	 * {@code RPOPLPUSH} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	class RPopLPushCommand extends KeyCommand {

		private final @Nullable ByteBuffer destination;

		private RPopLPushCommand(ByteBuffer key, @Nullable ByteBuffer destination) {

			super(key);
			this.destination = destination;
		}

		/**
		 * Creates a new {@link RPopLPushCommand} given a {@literal sourceKey}.
		 *
		 * @param sourceKey must not be {@literal null}.
		 * @return a new {@link RPopLPushCommand} for a {@literal sourceKey}.
		 */
		public static RPopLPushCommand from(ByteBuffer sourceKey) {

			Assert.notNull(sourceKey, "Source key must not be null!");

			return new RPopLPushCommand(sourceKey, null);
		}

		/**
		 * Applies the {@literal destinationKey}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param destinationKey must not be {@literal null}.
		 * @return a new {@link BPopCommand} with {@literal value} applied.
		 */
		public RPopLPushCommand to(ByteBuffer destinationKey) {

			Assert.notNull(destinationKey, "Destination key must not be null!");

			return new RPopLPushCommand(getKey(), destinationKey);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getDestination() {
			return destination;
		}
	}

	/**
	 * Remove the last element from list at {@literal source}, append it to {@literal destination} and return its value.
	 *
	 * @param source must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	default Mono<ByteBuffer> rPopLPush(ByteBuffer source, ByteBuffer destination) {

		Assert.notNull(source, "Source must not be null!");
		Assert.notNull(destination, "Destination must not be null!");

		return rPopLPush(Mono.just(RPopLPushCommand.from(source).to(destination))) //
				.next() //
				.map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove the last element from list at {@link RPopLPushCommand#getKey()}, append it to
	 * {@link RPopLPushCommand#getDestination()} and return its value.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/rpoplpush">Redis Documentation: RPOPLPUSH</a>
	 */
	Flux<ByteBufferResponse<RPopLPushCommand>> rPopLPush(Publisher<RPopLPushCommand> commands);

	/**
	 * {@code BRPOPLPUSH} command parameters.
	 *
	 * @author Christoph Strobl
	 * @see <a href="http://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	class BRPopLPushCommand extends KeyCommand {

		private final @Nullable ByteBuffer destination;
		private final Duration timeout;

		private BRPopLPushCommand(ByteBuffer key, @Nullable ByteBuffer destination, Duration timeout) {

			super(key);

			this.destination = destination;
			this.timeout = timeout;
		}

		/**
		 * Creates a new {@link BRPopLPushCommand} given a {@literal sourceKey}.
		 *
		 * @param sourceKey must not be {@literal null}.
		 * @return a new {@link BRPopLPushCommand} for a {@literal sourceKey}.
		 */
		public static BRPopLPushCommand from(ByteBuffer sourceKey) {

			Assert.notNull(sourceKey, "Source key must not be null!");

			return new BRPopLPushCommand(sourceKey, null, Duration.ZERO);
		}

		/**
		 * Applies the {@literal destinationKey}. Constructs a new command instance with all previously configured
		 * properties.
		 *
		 * @param destinationKey must not be {@literal null}.
		 * @return a new {@link BRPopLPushCommand} with {@literal value} applied.
		 */
		public BRPopLPushCommand to(ByteBuffer destinationKey) {

			Assert.notNull(destinationKey, "Destination key must not be null!");

			return new BRPopLPushCommand(getKey(), destinationKey, timeout);
		}

		/**
		 * Applies a {@link Duration timeout}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param timeout must not be {@literal null}.
		 * @return a new {@link BRPopLPushCommand} with {@link Duration timeout} applied.
		 */
		public BRPopLPushCommand blockingFor(Duration timeout) {

			Assert.notNull(timeout, "Timeout must not be null!");

			return new BRPopLPushCommand(getKey(), destination, timeout);
		}

		/**
		 * @return can be {@literal null}.
		 */
		@Nullable
		public ByteBuffer getDestination() {
			return destination;
		}

		/**
		 * @return never {@literal null}.
		 */
		public Duration getTimeout() {
			return timeout;
		}
	}

	/**
	 * Remove the last element from list at {@literal source}, append it to {@literal destination} and return its value.
	 * <b>Blocks connection</b> until element available or {@literal timeout} reached. <br />
	 *
	 * @param source must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	default Mono<ByteBuffer> bRPopLPush(ByteBuffer source, ByteBuffer destination, Duration timeout) {

		Assert.notNull(source, "Source must not be null!");
		Assert.notNull(destination, "Destination must not be null!");

		return bRPopLPush(Mono.just(BRPopLPushCommand.from(source).to(destination).blockingFor(timeout))).next()
				.map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove the last element from list at {@link BRPopLPushCommand#getKey()}, append it to
	 * {@link BRPopLPushCommand#getDestination()} and return its value. <br />
	 * <b>Blocks connection</b> until element available or {@link BRPopLPushCommand#getTimeout()} reached.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/brpoplpush">Redis Documentation: BRPOPLPUSH</a>
	 */
	Flux<ByteBufferResponse<BRPopLPushCommand>> bRPopLPush(Publisher<BRPopLPushCommand> commands);
}
