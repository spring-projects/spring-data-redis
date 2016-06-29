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
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveKeyCommands {

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> exists(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return exists(Mono.just(new KeyCommand(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<KeyCommand>> exists(Publisher<KeyCommand> keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<DataType> type(ByteBuffer key) {

		Assert.notNull(key, "key must not be null");

		return type(Mono.just(new KeyCommand(key))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<CommandResponse<KeyCommand, DataType>> type(Publisher<KeyCommand> keys);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> keys(ByteBuffer pattern) {

		Assert.notNull(pattern, "pattern must not be null");

		return keys(Mono.just(pattern)).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return
	 */
	Mono<ByteBuffer> randomKey();

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param patterns must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<ByteBuffer, ByteBuffer>> keys(Publisher<ByteBuffer> patterns);

	/**
	 * @author Christoph Strobl
	 */
	class RenameCommand extends KeyCommand {

		private ByteBuffer newName;

		private RenameCommand(ByteBuffer key, ByteBuffer newName) {

			super(key);
			this.newName = newName;
		}

		public static ReactiveKeyCommands.RenameCommand key(ByteBuffer key) {
			return new RenameCommand(key, null);
		}

		public ReactiveKeyCommands.RenameCommand to(ByteBuffer newName) {
			return new RenameCommand(getKey(), newName);
		}

		public ByteBuffer getNewName() {
			return newName;
		}
	}

	/**
	 * Rename key {@code oleName} to {@code newName}.
	 *
	 * @param key must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> rename(ByteBuffer key, ByteBuffer newName) {

		Assert.notNull(key, "key must not be null");

		return rename(Mono.just(RenameCommand.key(key).to(newName))).next().map(BooleanResponse::getOutput);
	}

	Flux<BooleanResponse<ReactiveKeyCommands.RenameCommand>> rename(Publisher<ReactiveKeyCommands.RenameCommand> cmd);

	/**
	 * Rename key {@code oleName} to {@code newName} only if {@code newName} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> renameNX(ByteBuffer key, ByteBuffer newName) {

		Assert.notNull(key, "key must not be null");

		return renameNX(Mono.just(RenameCommand.key(key).to(newName))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Rename key {@code oleName} to {@code newName} only if {@code newName} does not exist.
	 *
	 * @param keys must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<ReactiveKeyCommands.RenameCommand>> renameNX(
			Publisher<ReactiveKeyCommands.RenameCommand> command);

	/**
	 * Delete {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> del(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return del(Mono.just(new KeyCommand(key))).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete {@literal keys} one by one.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link DelResponse} holding the {@literal key} removed along with the deletion result.
	 */
	Flux<NumericResponse<KeyCommand, Long>> del(Publisher<KeyCommand> keys);

	/**
	 * Delete multiple {@literal keys} one in one batch.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	default Mono<Long> mDel(List<ByteBuffer> keys) {

		Assert.notEmpty(keys, "Keys must not be empty or null!");

		return mDel(Mono.just(keys)).next().map(NumericResponse::getOutput);
	}

	/**
	 * Delete multiple {@literal keys} in batches.
	 *
	 * @param keys must not be {@literal null}.
	 * @return {@link Flux} of {@link MDelResponse} holding the {@literal keys} removed along with the deletion result.
	 */
	Flux<NumericResponse<List<ByteBuffer>, Long>> mDel(Publisher<List<ByteBuffer>> keys);
}
