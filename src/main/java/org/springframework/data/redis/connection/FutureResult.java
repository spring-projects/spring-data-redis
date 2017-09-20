/*
 * Copyright 2013-2017 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

/**
 * The result of an asynchronous operation
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The data type of the object that holds the future result (usually of type Future)
 */
public abstract class FutureResult<T> {

	protected T resultHolder;

	protected boolean status = false;

	@SuppressWarnings("rawtypes") //
	protected @Nullable Converter converter;

	public FutureResult(T resultHolder) {
		this.resultHolder = resultHolder;
	}

	@SuppressWarnings("rawtypes")
	public FutureResult(T resultHolder, Converter converter) {
		this.resultHolder = resultHolder;
		this.converter = converter;
	}

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
			return null;
		}

		return (converter != null) ? converter.convert(result) : result;
	}

	@SuppressWarnings("rawtypes")
	@Nullable
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
}
