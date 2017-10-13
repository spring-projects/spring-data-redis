/*
 * Copyright 2017 the original author or authors.
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
class LettuceResult<T, S> extends FutureResult<RedisCommand<?, T, ?>> {

	private final boolean convertPipelineAndTxResults;

	<T> LettuceResult(Future<T> resultHolder) {
		this(resultHolder, false, val -> val);
	}

	<T> LettuceResult(Future<T> resultHolder, boolean convertPipelineAndTxResults, @Nullable Converter<T, ?> converter) {
		this(resultHolder, () -> null, convertPipelineAndTxResults, converter);
	}

	<T> LettuceResult(Future<T> resultHolder, Supplier<S> defaultReturnValue, boolean convertPipelineAndTxResults,
			@Nullable Converter<T, ?> converter) {

		super((RedisCommand) resultHolder, converter, defaultReturnValue);
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#get()
	 * @return
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T) getResultHolder().getOutput().get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#seeksConversion()
	 * @return
	 */
	@Override
	public boolean seeksConversion() {
		return convertPipelineAndTxResults && converter != null;
	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a throw away status result.
	 */
	static class LettuceStatusResult extends LettuceResult {

		@SuppressWarnings("rawtypes")
		LettuceStatusResult(Future resultHolder) {
			super(resultHolder);
			setStatus(true);
		}

		<T> LettuceStatusResult(Future<T> resultHolder, boolean convertPipelineAndTxResults, Converter<T, ?> converter) {
			super(resultHolder, convertPipelineAndTxResults, converter);
			setStatus(true);
		}
	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a transaction result.
	 */
	static class LettuceTxResult<T> extends FutureResult<Object> {

		private final boolean convertPipelineAndTxResults;

		LettuceTxResult(T resultHolder) {
			this(resultHolder, false, val -> val);
		}

		LettuceTxResult(T resultHolder, boolean convertPipelineAndTxResults, Converter<?, ?> converter) {
			this(resultHolder, () -> null, convertPipelineAndTxResults, converter);
		}

		LettuceTxResult(T resultHolder, Supplier<Object> defaultReturnValue, boolean convertPipelineAndTxResults,
				Converter<?, ?> converter) {
			super(resultHolder, converter, defaultReturnValue);
			this.convertPipelineAndTxResults = convertPipelineAndTxResults;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object get() {
			return getResultHolder();
		}

		@Override
		public boolean seeksConversion() {
			return convertPipelineAndTxResults && converter != null;
		}

	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a throw away status result.
	 */
	static class LettuceTxStatusResult extends LettuceTxResult {

		LettuceTxStatusResult(Object resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

	/**
	 * Builder for constructing {@link LettuceResult}.
	 *
	 * @param <T>
	 * @param <S>
	 * @since 2.1
	 */
	static class LettuceResultBuilder<T, S> {

		private final Object response;
		private Converter<T, ?> converter;
		private boolean convertPipelineAndTxResults = false;
		private Supplier<?> nullValueDefault = () -> null;

		LettuceResultBuilder(Object response) {

			this.response = response;
			this.converter = (source) -> source;
		}

		static <T> LettuceResultBuilder<T, ?> forResponse(Future<T> response) {
			return new LettuceResultBuilder<>(response);
		}

		static <T> LettuceResultBuilder<T, ?> forResponse(T response) {
			return new LettuceResultBuilder<>(response);
		}

		<S> LettuceResultBuilder<T, S> mappedWith(Converter<T, S> converter) {

			this.converter = converter;
			return (LettuceResultBuilder<T, S>) this;
		}

		<S> LettuceResultBuilder<T, S> defaultNullTo(S value) {
			return (defaultNullTo(() -> value));
		}

		<S> LettuceResultBuilder<T, S> defaultNullTo(Supplier<S> value) {

			this.nullValueDefault = value;
			return (LettuceResultBuilder<T, S>) this;
		}

		LettuceResultBuilder<T, S> convertPipelineAndTxResults(boolean flag) {

			convertPipelineAndTxResults = flag;
			return this;
		}

		LettuceResult<T, S> build() {
			return new LettuceResult((Future<T>) response, nullValueDefault, convertPipelineAndTxResults, converter);
		}

		LettuceTxResult<T> buildTxResult() {

			return new LettuceTxResult(response, nullValueDefault, convertPipelineAndTxResults, converter);
		}

		LettuceResult buildStatusResult() {
			return new LettuceStatusResult((Future<T>) response);
		}
	}
}
