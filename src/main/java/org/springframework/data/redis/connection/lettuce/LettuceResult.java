/*
 * Copyright 2017-2018 the original author or authors.
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

import io.lettuce.core.protocol.RedisCommand;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.lang.Nullable;

/**
 * Lettuce specific {@link FutureResult} implementation. <br />
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
@SuppressWarnings("rawtypes")
class LettuceResult<T, R> extends FutureResult<RedisCommand<?, T, ?>> {

	private final boolean convertPipelineAndTxResults;

	@SuppressWarnings("unchecked")
	LettuceResult(Future<T> resultHolder) {
		this(resultHolder, false, (Converter) val -> val);
	}

	LettuceResult(Future<T> resultHolder, boolean convertPipelineAndTxResults, @Nullable Converter<T, R> converter) {
		this(resultHolder, () -> null, convertPipelineAndTxResults, converter);
	}

	@SuppressWarnings("unchecked")
	LettuceResult(Future<T> resultHolder, Supplier<R> defaultReturnValue, boolean convertPipelineAndTxResults,
			@Nullable Converter<T, R> converter) {

		super((RedisCommand) resultHolder, converter, defaultReturnValue);
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#get()
	 */
	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public T get() {
		return (T) getResultHolder().getOutput().get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#conversionRequired()
	 */
	@Override
	public boolean conversionRequired() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a throw away status result.
	 */
	static class LettuceStatusResult<T, R> extends LettuceResult<T, R> {

		@SuppressWarnings("unchecked")
		LettuceStatusResult(Future<T> resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

	/**
	 * Builder for constructing {@link LettuceResult}.
	 *
	 * @param <T>
	 * @param <R>
	 * @since 2.1
	 */
	static class LettuceResultBuilder<T, R> {

		private final Future<T> response;
		private Converter<T, R> converter;
		private boolean convertPipelineAndTxResults = false;
		private Supplier<R> nullValueDefault = () -> null;

		@SuppressWarnings("unchecked")
		LettuceResultBuilder(Future<T> response) {

			this.response = response;
			this.converter = (source) -> (R) source;
		}

		/**
		 * Create a new {@link LettuceResultBuilder} given {@link Future}.
		 *
		 * @param response must not be {@literal null}.
		 * @param <T> native response type.
		 * @param <R> resulting response type.
		 * @return the new {@link LettuceResultBuilder}.
		 */
		static <T, R> LettuceResultBuilder<T, R> forResponse(Future<T> response) {
			return new LettuceResultBuilder<>(response);
		}

		/**
		 * Configure a {@link Converter} to convert between {@code T} and {@code R} types.
		 *
		 * @param converter must not be {@literal null}.
		 * @return {@code this} builder.
		 */
		LettuceResultBuilder<T, R> mappedWith(Converter<T, R> converter) {

			this.converter = converter;
			return this;
		}

		/**
		 * Configure a {@link Supplier} to map {@literal null} responses to a different value.
		 *
		 * @param supplier must not be {@literal null}.
		 * @return {@code this} builder.
		 */
		LettuceResultBuilder<T, R> defaultNullTo(Supplier<R> supplier) {

			this.nullValueDefault = supplier;
			return this;
		}

		LettuceResultBuilder<T, R> convertPipelineAndTxResults(boolean flag) {

			convertPipelineAndTxResults = flag;
			return this;
		}

		/**
		 * @return a new {@link LettuceResult} wrapper with configuration applied from this builder.
		 */
		LettuceResult<T, R> build() {
			return new LettuceResult<>(response, nullValueDefault, convertPipelineAndTxResults, converter);
		}

		/**
		 * @return a new {@link LettuceResult} wrapper for status results with configuration applied from this builder.
		 */
		LettuceResult<T, R> buildStatusResult() {
			return new LettuceStatusResult<>(response);
		}
	}
}
