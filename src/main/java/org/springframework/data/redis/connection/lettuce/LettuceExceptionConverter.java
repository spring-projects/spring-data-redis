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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandInterruptedException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.netty.channel.ChannelException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;

/**
 * Converts Lettuce Exceptions to {@link DataAccessException}s
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class LettuceExceptionConverter implements Converter<Exception, DataAccessException> {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	public DataAccessException convert(Exception ex) {

		if (ex instanceof ExecutionException || ex instanceof RedisCommandExecutionException) {

			if (ex.getCause() != ex && ex.getCause() instanceof Exception) {
				return convert((Exception) ex.getCause());
			}
			return new RedisSystemException("Error in execution", ex);
		}

		if (ex instanceof DataAccessException) {
			return (DataAccessException) ex;
		}

		if (ex instanceof RedisCommandInterruptedException) {
			return new RedisSystemException("Redis command interrupted", ex);
		}

		if (ex instanceof ChannelException || ex instanceof RedisConnectionException) {
			return new RedisConnectionFailureException("Redis connection failed", ex);
		}

		if (ex instanceof TimeoutException || ex instanceof RedisCommandTimeoutException) {
			return new QueryTimeoutException("Redis command timed out", ex);
		}

		if (ex instanceof RedisException) {
			return new RedisSystemException("Redis exception", ex);
		}

		return null;
	}
}
