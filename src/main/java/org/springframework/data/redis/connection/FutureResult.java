/*
 * Copyright 2013 the original author or authors.
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

/**
 * The result of an asynchronous operation
 *
 * @author Jennifer Hickey
 *
 * @param <T>
 *            The data type of the object that holds the future result (usually
 *            of type Future)
 */
abstract public class FutureResult<T> {

	protected T resultHolder;

	@SuppressWarnings("rawtypes")
	protected Converter converter;

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
	 * Converts the given result if a converter is specified, else returns the
	 * result
	 *
	 * @param result
	 *            The result to convert
	 * @return The converted result
	 */
	@SuppressWarnings("unchecked")
	public Object convert(Object result) {
		if (converter != null) {
			return converter.convert(result);
		}
		return result;
	}

	@SuppressWarnings("rawtypes")
	public Converter getConverter() {
		return converter;
	}

	/**
	 * @return The result of the operation
	 */
	abstract public Object get();
}
