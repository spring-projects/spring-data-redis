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

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveNumberCommands {

	/**
	 * Increment value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> incr(ByteBuffer key) {


			Assert.notNull(key, "key must not be null");

		return incr(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Increment value of {@code key} by 1.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> incr(Publisher<KeyCommand> keys);

	/**
	 * @author Christoph Strobl
	 */
	class IncrByCommand<T extends Number> extends KeyCommand {

		private T value;

		private IncrByCommand(ByteBuffer key, T value) {
			super(key);
			this.value = value;
		}

		public static <T extends Number> IncrByCommand<T> incr(ByteBuffer key) {
			return new IncrByCommand<T>(key, null);
		}

		public IncrByCommand<T> by(T value) {
			return new IncrByCommand<T>(getKey(), value);
		}

		public T getValue() {
			return value;
		}

	}

	/**
	 * Increment value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default <T extends Number> Mono<T> incrBy(ByteBuffer key, T value) {


			Assert.notNull(key, "key must not be null");
			Assert.notNull(value, "value must not be null");


		return incrBy(Mono.just(IncrByCommand.<T> incr(key).by(value))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Increment value of {@code key} by {@code value}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	<T extends Number> Flux<NumericResponse<ReactiveNumberCommands.IncrByCommand<T>, T>> incrBy(
			Publisher<ReactiveNumberCommands.IncrByCommand<T>> commands);

	/**
	 * @author Christoph Strobl
	 */
	class DecrByCommand<T extends Number> extends KeyCommand {

		private T value;

		private DecrByCommand(ByteBuffer key, T value) {
			super(key);
			this.value = value;
		}

		public static <T extends Number> ReactiveNumberCommands.DecrByCommand<T> decr(ByteBuffer key) {
			return new DecrByCommand<T>(key, null);
		}

		public ReactiveNumberCommands.DecrByCommand<T> by(T value) {
			return new DecrByCommand<T>(getKey(), value);
		}

		public T getValue() {
			return value;
		}

	}

	/**
	 * Decrement value of {@code key} by 1.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> decr(ByteBuffer key) {


			Assert.notNull(key, "key must not be null");


		return decr(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Decrement value of {@code key} by 1.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<NumericResponse<KeyCommand, Long>> decr(Publisher<KeyCommand> keys);

	/**
	 * Decrement value of {@code key} by {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default <T extends Number> Mono<T> decrBy(ByteBuffer key, T value) {


			Assert.notNull(key, "key must not be null");
			Assert.notNull(value, "value must not be null");


		return decrBy(Mono.just(DecrByCommand.<T> decr(key).by(value))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Decrement value of {@code key} by {@code value}.
	 *
	 * @param keys must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	<T extends Number> Flux<NumericResponse<ReactiveNumberCommands.DecrByCommand<T>, T>> decrBy(
			Publisher<ReactiveNumberCommands.DecrByCommand<T>> commands);

	/**
	 * @author Christoph Strobl
	 */
	class HIncrByCommand<T extends Number> extends KeyCommand {

		private final ByteBuffer field;
		private final T value;

		private HIncrByCommand(ByteBuffer key, ByteBuffer field, T value) {

			super(key);
			this.field = field;
			this.value = value;
		}

		public static <T extends Number> HIncrByCommand<T> incr(ByteBuffer field) {
			return new HIncrByCommand<T>(null, field, null);
		}

		public HIncrByCommand<T> by(T value) {
			return new HIncrByCommand<T>(getKey(), field, value);
		}

		public HIncrByCommand<T> forKey(ByteBuffer key) {
			return new HIncrByCommand<T>(key, field, value);
		}

		public T getValue() {
			return value;
		}

		public ByteBuffer getField() {
			return field;
		}
	}

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code value}.
	 *
	 * @param key must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	default <T extends Number> Mono<T> hIncrBy(ByteBuffer key, ByteBuffer field, T value) {


			Assert.notNull(key, "key must not be null");
			Assert.notNull(field, "field must not be null");
			Assert.notNull(value, "value must not be null");


		return hIncrBy(Mono.just(HIncrByCommand.<T> incr(field).by(value).forKey(key))).next()
				.map(NumericResponse::getOutput);
	}

	/**
	 * Increment {@code value} of a hash {@code field} by the given {@code value}.
	 *
	 * @return
	 */
	<T extends Number> Flux<NumericResponse<HIncrByCommand<T>, T>> hIncrBy(Publisher<HIncrByCommand<T>> commands);

}
