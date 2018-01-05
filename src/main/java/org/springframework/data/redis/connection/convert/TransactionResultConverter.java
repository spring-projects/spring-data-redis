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
package org.springframework.data.redis.connection.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.FutureResult;

/**
 * Converts the results of transaction exec using a supplied Queue of {@link FutureResult}s. Converts any Exception
 * objects returned in the list as well, using the supplied Exception {@link Converter}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The type of {@link FutureResult} of the individual tx operations
 */
public class TransactionResultConverter<T> implements Converter<List<Object>, List<Object>> {

	private final Queue<FutureResult<T>> txResults;
	private final Converter<Exception, DataAccessException> exceptionConverter;

	public TransactionResultConverter(Queue<FutureResult<T>> txResults,
			Converter<Exception, DataAccessException> exceptionConverter) {

		this.txResults = txResults;
		this.exceptionConverter = exceptionConverter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(Object)
	 */
	@Override
	public List<Object> convert(List<Object> execResults) {

		if (execResults.size() != txResults.size()) {

			throw new IllegalArgumentException(
					"Incorrect number of transaction results. Expected: " + txResults.size() + " Actual: " + execResults.size());
		}

		List<Object> convertedResults = new ArrayList<>();

		for (Object result : execResults) {
			FutureResult<T> futureResult = txResults.remove();
			if (result instanceof Exception) {

				Exception source = (Exception) result;
				DataAccessException convertedException = exceptionConverter.convert(source);
				throw convertedException != null ? convertedException
						: new RedisSystemException("Error reading future result.", source);
			}
			if (!(futureResult.isStatus())) {
				convertedResults.add(futureResult.conversionRequired() ? futureResult.convert(result) : result);
			}
		}

		return convertedResults;
	}
}
