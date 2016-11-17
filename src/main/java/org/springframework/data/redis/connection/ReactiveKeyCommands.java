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
 * Redis Key commands executed using reactive infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveKeyCommands {

	/**
	 * Determine if given {@literal key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> exists(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return exists(Mono.just(new KeyCommand(key))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Determine if given {@literal key} exists.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<KeyCommand>> exists(Publisher<KeyCommand> keys);

	/**
	 * Determine the type stored at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	default Mono<DataType> type(ByteBuffer key) {

		Assert.notNull(key, "Key must not be null!");

		return type(Mono.just(new KeyCommand(key))).next().map(CommandResponse::getOutput);
	}

	/**
	 * Determine the type stored at {@literal key}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	Flux<CommandResponse<KeyCommand, DataType>> type(Publisher<KeyCommand> keys);

	/**
	 * Find all keys matching the given {@literal pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return
	 */
	default Mono<List<ByteBuffer>> keys(ByteBuffer pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		return keys(Mono.just(pattern)).next().map(MultiValueResponse::getOutput);
	}

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return
	 */
	Mono<ByteBuffer> randomKey();

	/**
	 * Find all keys matching the given {@literal pattern}.
	 *
	 * @param patterns must not be {@literal null}.
	 * @return
	 */
	Flux<MultiValueResponse<ByteBuffer, ByteBuffer>> keys(Publisher<ByteBuffer> patterns);

	/**
	 * {@code RENAME} command parameters.
	 *
	 * @author Christoph Strobl
	 */
	class RenameCommand extends KeyCommand {

		private ByteBuffer newName;

		private RenameCommand(ByteBuffer key, ByteBuffer newName) {

			super(key);

			this.newName = newName;
		}

		/**
		 * Creates a new {@link RenameCommand} given a {@link ByteBuffer key}.
		 *
		 * @param key must not be {@literal null}.
		 * @return a new {@link RenameCommand} for {@link ByteBuffer key}.
		 */
		public static RenameCommand key(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");

			return new RenameCommand(key, null);
		}

		/**
		 * Applies the {@literal newName}. Constructs a new command instance with all previously configured properties.
		 *
		 * @param newName must not be {@literal null}.
		 * @return a new {@link RenameCommand} with {@literal newName} applied.
		 */
		public RenameCommand to(ByteBuffer newName) {

			Assert.notNull(newName, "New name must not be null!");

			return new RenameCommand(getKey(), newName);
		}

		/**
		 * @return
		 */
		public ByteBuffer getNewName() {
			return newName;
		}
	}

	/**
	 * Rename key {@literal oleName} to {@literal newName}.
	 *
	 * @param key must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> rename(ByteBuffer key, ByteBuffer newName) {

		Assert.notNull(key, "Key must not be null!");

		return rename(Mono.just(RenameCommand.key(key).to(newName))).next().map(BooleanResponse::getOutput);
	}

	Flux<BooleanResponse<RenameCommand>> rename(Publisher<RenameCommand> cmd);

	/**
	 * Rename key {@literal oleName} to {@literal newName} only if {@literal newName} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param newName must not be {@literal null}.
	 * @return
	 */
	default Mono<Boolean> renameNX(ByteBuffer key, ByteBuffer newName) {

		Assert.notNull(key, "Key must not be null!");

		return renameNX(Mono.just(RenameCommand.key(key).to(newName))).next().map(BooleanResponse::getOutput);
	}

	/**
	 * Rename key {@literal oleName} to {@literal newName} only if {@literal newName} does not exist.
	 *
	 * @param command must not be {@literal null}.
	 * @return
	 */
	Flux<BooleanResponse<RenameCommand>> renameNX(Publisher<RenameCommand> command);

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
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal key} removed along with the deletion result.
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
	 * @return {@link Flux} of {@link NumericResponse} holding the {@literal keys} removed along with the deletion result.
	 */
	Flux<NumericResponse<List<ByteBuffer>, Long>> mDel(Publisher<List<ByteBuffer>> keys);
}
