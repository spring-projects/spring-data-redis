/*
 * Copyright 2013-2018 the original author or authors.
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

import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

/**
 * The result of an asynchronous operation
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The data type of the object that holds the future result (usually type of the
 *          {@link java.util.concurrent.Future} or response wrapper).
 */
public abstract class FutureResult<T> {

	private T resultHolder;
	private final Supplier<?> defaultConversionResult;

	private boolean status = false;

	@SuppressWarnings("rawtypes") //
	protected Converter converter;

	/**
	 * Create new {@link FutureResult} for given object actually holding the result itself.
	 *
	 * @param resultHolder must not be {@literal null}.
	 */
	public FutureResult(T resultHolder) {
		this(resultHolder, val -> val);
	}

	/**
	 * Create new {@link FutureResult} for given object actually holding the result itself and a converter capable of
	 * transforming the result via {@link #convert(Object)}.
	 *
	 * @param resultHolder must not be {@literal null}.
	 * @param converter can be {@literal null} and will be defaulted to an identity converter {@code value -> value} to
	 *          preserve the original value.
	 */
	@SuppressWarnings("rawtypes")
	public FutureResult(T resultHolder, @Nullable Converter converter) {
		this(resultHolder, converter, () -> null);
	}

	/**
	 * Create new {@link FutureResult} for given object actually holding the result itself and a converter capable of
	 * transforming the result via {@link #convert(Object)}.
	 *
	 * @param resultHolder must not be {@literal null}.
	 * @param converter can be {@literal null} and will be defaulted to an identity converter {@code value -> value} to
	 *          preserve the original value.
	 * @param defaultConversionResult must not be {@literal null}.
	 * @since 2.1
	 */
	public FutureResult(T resultHolder, @Nullable Converter converter, Supplier<?> defaultConversionResult) {

		this.resultHolder = resultHolder;
		this.converter = converter != null ? converter : val -> val;
		this.defaultConversionResult = defaultConversionResult;
	}

	/**
	 * Get the object holding the actual result.
	 *
	 * @return never {@literal null}.
	 * @since 1.1
	 */
	public T getResultHolder() {
		return resultHolder;
	}

	/**
	 * Converts the given result if a converter is specified, else returns the result
	 *
	 * @param result The result to convert. Can be {@literal null}.
	 * @return The converted result or {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	public Object convert(@Nullable Object result) {

		if (result == null) {
			return computeDefaultResult(null);
		}

		return computeDefaultResult(converter.convert(result));
	}

	@Nullable
	private Object computeDefaultResult(@Nullable Object source) {
		return source != null ? source : defaultConversionResult.get();
	}

	@SuppressWarnings("rawtypes")
	public Converter getConverter() {
		return converter;
	}

	/**
	 * Indicates if this result is the status of an operation. Typically status results will be discarded on conversion.
	 *
	 * @return true if this is a status result (i.e. OK)
	 */
	public boolean isStatus() {
		return status;
	}

	/**
	 * Indicates if this result is the status of an operation. Typically status results will be discarded on conversion.
	 */
	public void setStatus(boolean status) {
		this.status = status;
	}

	/**
	 * @return The result of the operation. Can be {@literal null}.
	 */
	@Nullable
	public abstract Object get();

	/**
	 * Indicate whether or not the actual result needs to be {@link #convert(Object) converted} before handing over.
	 *
	 * @return {@literal true} if result conversion is required.
	 * @since 2.1
	 */
	public abstract boolean conversionRequired();
}
