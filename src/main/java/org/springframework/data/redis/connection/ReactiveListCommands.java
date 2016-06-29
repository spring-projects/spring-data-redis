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
package org.springframework.data.redis.connection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.Command;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.RangeCommand;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
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
	 * @author Christoph Strobl
	 */
	class PushCommand extends KeyCommand {

		private List<ByteBuffer> values;
		private boolean upsert;
		private Direction direction;

		private PushCommand(ByteBuffer key, List<ByteBuffer> values, Direction direction, boolean upsert) {

			super(key);
			this.values = values;
			this.upsert = upsert;
			this.direction = direction;
		}

		public static PushCommand right() {
			return new PushCommand(null, null, Direction.RIGHT, true);
		}

		public static PushCommand left() {
			return new PushCommand(null, null, Direction.LEFT, true);
		}

		public PushCommand value(ByteBuffer value) {
			return new PushCommand(null, Collections.singletonList(value), direction, upsert);
		}

		public PushCommand values(List<ByteBuffer> values) {
			return new PushCommand(null, values, direction, upsert);
		}

		public PushCommand to(ByteBuffer key) {
			return new PushCommand(key, values, direction, upsert);
		}

		public PushCommand ifExists() {
			return new PushCommand(getKey(), values, direction, false);
		}

		public List<ByteBuffer> getValues() {
			return values;
		}

		public boolean getUpsert() {
			return upsert;
		}

		public Direction getDirection() {
			return direction;
		}
	}

	/**
	 * Append {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> rPush(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "command must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return push(Mono.just(PushCommand.right().values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Append {@code values} to {@code key} only if {@code key} already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> rPushX(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "command must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return push(Mono.just(PushCommand.right().value(value).to(key).ifExists())).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@code values} to {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lPush(ByteBuffer key, List<ByteBuffer> values) {

		Assert.notNull(key, "command must not be null!");
		Assert.notNull(values, "Values must not be null!");

		return push(Mono.just(PushCommand.left().values(values).to(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@code value} to {@code key} if {@code key} already exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lPushX(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "command must not be null!");
		Assert.notNull(value, "Value must not be null!");

		return push(Mono.just(PushCommand.left().value(value).to(key).ifExists())).next().map(NumericResponse::getOutput);
	}

	/**
	 * Prepend {@link PushCommand#getValues()} to {@link PushCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<PushCommand, Long>> push(Publisher<PushCommand> commands);

	/**
	 * Get the size of list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lLen(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return lLen(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Get the size of list stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> lLen(Publisher<KeyCommand> commands);

	/**
	 * Get elements between {@code begin} and {@code end} from list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 */
	default Mono<List<ByteBuffer>> lRange(ByteBuffer key, long start, long end) {

		Assert.notNull(key, "key must not be null");

		return lRange(Mono.just(RangeCommand.key(key).fromIndex(start).toIndex(end))).next()
				.map(MultiValueResponse::getOutput);
	}

	/**
	 * Get elements in {@link RangeCommand#getRange()} from list at {@link RangeCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<RangeCommand, ByteBuffer>> lRange(Publisher<RangeCommand> commands);

	/**
	 * Trim list at {@code key} to elements between {@code begin} and {@code end}.
	 *
	 * @param key must not be {@literal null}.
	 * @param start
	 * @param end
	 * @return
	 */
	default Mono<Boolean> lTrim(ByteBuffer key, long start, long end) {

		Assert.notNull(key, "key must not be null");

		return lTrim(Mono.just(RangeCommand.key(key).fromIndex(start).toIndex(end))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Trim list at {@link RangeCommand#getKey()} to elements within {@link RangeCommand#getRange()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<RangeCommand>> lTrim(Publisher<RangeCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class LIndexCommand extends KeyCommand {

		private final Long index;

		private LIndexCommand(ByteBuffer key, Long index) {

			super(key);
			this.index = index;
		}

		public static LIndexCommand elementAt(Long index) {
			return new LIndexCommand(null, index);
		}

		public LIndexCommand from(ByteBuffer key) {
			return new LIndexCommand(key, index);
		}

		public Long getIndex() {
			return index;
		}
	}

	/**
	 * Get element at {@code index} form list at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @return
	 */
	default Mono<ByteBuffer> lIndex(ByteBuffer key, long index) {

		Assert.notNull(key, "key must not be null");

		return lIndex(Mono.just(LIndexCommand.elementAt(index).from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Get element at {@link LIndexCommand#getIndex()} form list at {@link LIndexCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<ByteBufferResponse<LIndexCommand>> lIndex(Publisher<LIndexCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class LInsertCommand extends KeyCommand {

		private final Position position;
		private final ByteBuffer pivot;
		private final ByteBuffer value;

		public LInsertCommand(ByteBuffer key, Position position, ByteBuffer pivot, ByteBuffer value) {

			super(key);
			this.position = position;
			this.pivot = pivot;
			this.value = value;
		}

		public static LInsertCommand value(ByteBuffer value) {
			return new LInsertCommand(null, null, null, value);
		}

		public LInsertCommand before(ByteBuffer pivot) {
			return new LInsertCommand(getKey(), Position.BEFORE, pivot, value);
		}

		public LInsertCommand after(ByteBuffer pivot) {
			return new LInsertCommand(getKey(), Position.AFTER, pivot, value);
		}

		public LInsertCommand forKey(ByteBuffer key) {
			return new LInsertCommand(key, position, pivot, value);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Position getPosition() {
			return position;
		}

		public ByteBuffer getPivot() {
			return pivot;
		}
	}

	/**
	 * Insert {@code value} {@link Position#BEFORE} or {@link Position#AFTER} existing {@code pivot} for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lInsert(ByteBuffer key, Position position, ByteBuffer pivot, ByteBuffer value) {

		Assert.notNull(key, "key must not be null!");
		Assert.notNull(position, "position must not be null!");
		Assert.notNull(pivot, "pivot must not be null!");
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
	 */
	Flux<NumericResponse<LInsertCommand, Long>> lInsert(Publisher<LInsertCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class LSetCommand extends KeyCommand {

		private final Long index;
		private final ByteBuffer value;

		private LSetCommand(ByteBuffer key, Long index, ByteBuffer value) {

			super(key);
			this.index = index;
			this.value = value;
		}

		public static LSetCommand elementAt(Long index) {
			return new LSetCommand(null, index, null);
		}

		public LSetCommand to(ByteBuffer value) {
			return new LSetCommand(getKey(), index, value);
		}

		public LSetCommand forKey(ByteBuffer key) {
			return new LSetCommand(key, index, value);
		}

		public ByteBuffer getValue() {
			return value;
		}

		public Long getIndex() {
			return index;
		}
	}

	/**
	 * Set the {@code value} list element at {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param index
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> lSet(ByteBuffer key, long index, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(value, "value must not be null");

		return lSet(Mono.just(LSetCommand.elementAt(index).to(value).forKey(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Set the {@link LSetCommand#getValue()} list element at {@link LSetCommand#getKey()}.
	 *
	 * @param commands
	 * @return
	 */
	Flux<BooleanResponse<LSetCommand>> lSet(Publisher<LSetCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class LRemCommand extends KeyCommand {

		private final Long count;
		private final ByteBuffer value;

		private LRemCommand(ByteBuffer key, Long count, ByteBuffer value) {
			super(key);
			this.count = count;
			this.value = value;
		}

		public static LRemCommand all() {
			return new LRemCommand(null, 0L, null);
		}

		public static LRemCommand first(Long count) {
			return new LRemCommand(null, count, null);
		}

		public static LRemCommand last(Long count) {

			Long value = count < 0L ? count : Math.negateExact(count);
			return new LRemCommand(null, value, null);
		}

		public LRemCommand occurancesOf(ByteBuffer value) {
			return new LRemCommand(getKey(), count, value);
		}

		public LRemCommand from(ByteBuffer key) {
			return new LRemCommand(key, count, value);
		}

		public Long getCount() {
			return count;
		}

		public ByteBuffer getValue() {
			return value;
		}
	}

	/**
	 * Removes all occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lRem(ByteBuffer key, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(value, "value must not be null");

		return lRem(Mono.just(LRemCommand.all().occurancesOf(value).from(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Removes the first {@code count} occurrences of {@code value} from the list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param count must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> lRem(ByteBuffer key, Long count, ByteBuffer value) {

		Assert.notNull(key, "key must not be null");
		Assert.notNull(count, "count must not be null");
		Assert.notNull(value, "value must not be null");

		return lRem(Mono.just(LRemCommand.first(count).occurancesOf(value).from(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Removes the {@link LRemCommand#getCount()} occurrences of {@link LRemCommand#getValue()} from the list stored at
	 * {@link LRemCommand#getKey()}.
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<LRemCommand, Long>> lRem(Publisher<LRemCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class PopCommand extends KeyCommand {

		private final Direction direction;

		private PopCommand(ByteBuffer key, Direction direction) {

			super(key);
			this.direction = direction;
		}

		public static PopCommand right() {
			return new PopCommand(null, Direction.RIGHT);
		}

		public static PopCommand left() {
			return new PopCommand(null, Direction.LEFT);
		}

		public PopCommand from(ByteBuffer key) {
			return new PopCommand(key, direction);
		}

		public Direction getDirection() {
			return direction;
		}

	}

	/**
	 * Removes and returns first element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> lPop(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return pop(Mono.just(PopCommand.left().from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Removes and returns last element in list stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> rPop(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return pop(Mono.just(PopCommand.right().from(key))).next().map(ByteBufferResponse::getOutput);
	}

	/**
	 * Removes and returns last element in list stored at {@link KeyCommand#getKey()}
	 *
	 * @param commands must not be {@literal null}.
	 * @return
	 */
	Flux<ByteBufferResponse<PopCommand>> pop(Publisher<PopCommand> commands);

	/**
	 * @author Christoph Strobl
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

		public static BPopCommand right() {
			return new BPopCommand(null, Duration.ZERO, Direction.RIGHT);
		}

		public static BPopCommand left() {
			return new BPopCommand(null, Duration.ZERO, Direction.LEFT);
		}

		public BPopCommand from(List<ByteBuffer> keys) {
			return new BPopCommand(keys, Duration.ZERO, direction);
		}

		public BPopCommand blockingFor(Duration timeout) {
			return new BPopCommand(keys, timeout, direction);
		}

		@Override
		public ByteBuffer getKey() {
			return null;
		}

		public List<ByteBuffer> getKeys() {
			return keys;
		}

		public Duration getTimeout() {
			return timeout;
		}

		public Direction getDirection() {
			return direction;
		}

	}

	/**
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
	 * @author Christoph Strobl
	 */
	class PopResponse extends CommandResponse<BPopCommand, PopResult> {

		public PopResponse(BPopCommand input, PopResult output) {
			super(input, output);
		}

	}

	/**
	 * Removes and returns first element from lists stored at {@code keys}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param keys must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 */
	default Mono<PopResult> blPop(List<ByteBuffer> keys, Duration timeout) {

		Assert.notNull(keys, "keys must not be null.");
		Assert.notNull(timeout, "timeout must not be null.");

		return bPop(Mono.just(BPopCommand.left().from(keys).blockingFor(timeout))).next().map(PopResponse::getOutput);
	}

	/**
	 * Removes and returns last element from lists stored at {@code keys}. <br>
	 * <b>Blocks connection</b> until element available or {@code timeout} reached.
	 *
	 * @param keys must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 */
	default Mono<PopResult> brPop(List<ByteBuffer> keys, Duration timeout) {

		Assert.notNull(keys, "keys must not be null.");
		Assert.notNull(timeout, "timeout must not be null.");

		return bPop(Mono.just(BPopCommand.right().from(keys).blockingFor(timeout))).next().map(PopResponse::getOutput);
	}

	/**
	 * Removes and returns the top {@link BPopCommand#getDirection()} element from lists stored at
	 * {@link BPopCommand#getKeys()}.<br>
	 * <b>Blocks connection</b> until element available or {@link BPopCommand#getTimeout()} reached.
	 *
	 * @param commands
	 * @return
	 */
	Flux<PopResponse> bPop(Publisher<BPopCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class RPopLPushCommand extends KeyCommand {

		private final ByteBuffer destination;

		private RPopLPushCommand(ByteBuffer key, ByteBuffer destination) {

			super(key);
			this.destination = destination;
		}

		public static RPopLPushCommand from(ByteBuffer sourceKey) {
			return new RPopLPushCommand(sourceKey, null);
		}

		public RPopLPushCommand to(ByteBuffer destinationKey) {
			return new RPopLPushCommand(getKey(), destinationKey);
		}

		public ByteBuffer getDestination() {
			return destination;
		}

	}

	/**
	 * Remove the last element from list at {@code source}, append it to {@code destination} and return its value.
	 *
	 * @param source must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> rPopLPush(ByteBuffer source, ByteBuffer destination) {

		Assert.notNull(source, "source must not be null");
		Assert.notNull(destination, "destination must not be null");

		return rPopLPush(Mono.just(RPopLPushCommand.from(source).to(destination))).next()
				.map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove the last element from list at {@link RPopLPushCommand#getKey()}, append it to
	 * {@link RPopLPushCommand#getDestination()} and return its value.
	 *
	 * @param source must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 */
	Flux<ByteBufferResponse<RPopLPushCommand>> rPopLPush(Publisher<RPopLPushCommand> commands);

	/**
	 * @author Christoph Strobl
	 */
	class BRPopLPushCommand extends KeyCommand {

		private final ByteBuffer destination;
		private final Duration timeout;

		private BRPopLPushCommand(ByteBuffer key, ByteBuffer destination, Duration timeout) {

			super(key);
			this.destination = destination;
			this.timeout = timeout;
		}

		public static BRPopLPushCommand from(ByteBuffer sourceKey) {
			return new BRPopLPushCommand(sourceKey, null, null);
		}

		public BRPopLPushCommand to(ByteBuffer destinationKey) {
			return new BRPopLPushCommand(getKey(), destinationKey, timeout);
		}

		public BRPopLPushCommand blockingFor(Duration timeout) {
			return new BRPopLPushCommand(getKey(), destination, timeout);
		}

		public ByteBuffer getDestination() {
			return destination;
		}

		public Duration getTimeout() {
			return timeout;
		}
	}

	/**
	 * Remove the last element from list at {@code source}, append it to {@code destination} and return its value.
	 * <b>Blocks connection</b> until element available or {@code timeout} reached. <br />
	 *
	 * @param source must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 */
	default Mono<ByteBuffer> bRPopLPush(ByteBuffer source, ByteBuffer destination, Duration timeout) {

		Assert.notNull(source, "source must not be null");
		Assert.notNull(destination, "destination must not be null");

		return bRPopLPush(Mono.just(BRPopLPushCommand.from(source).to(destination).blockingFor(timeout))).next()
				.map(ByteBufferResponse::getOutput);
	}

	/**
	 * Remove the last element from list at {@link BRPopLPushCommand#getKey()}, append it to
	 * {@link BRPopLPushCommand#getDestination()} and return its value. <br />
	 * <b>Blocks connection</b> until element available or {@link BRPopLPushCommand#getTimeout()} reached.
	 *
	 * @param source must not be {@literal null}.
	 * @param destination must not be {@literal null}.
	 * @return
	 */
	Flux<ByteBufferResponse<BRPopLPushCommand>> bRPopLPush(Publisher<BRPopLPushCommand> commands);
}
