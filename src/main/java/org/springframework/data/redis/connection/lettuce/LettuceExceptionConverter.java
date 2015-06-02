/*
 * Copyright 2013-2014 the original author or authors.
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

import java.util.concurrent.TimeoutException;

import com.lambdaworks.redis.RedisCommandInterruptedException;
import com.lambdaworks.redis.RedisCommandTimeoutException;
import com.lambdaworks.redis.RedisConnectionException;
import com.lambdaworks.redis.RedisException;
import io.netty.channel.ChannelException;
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
 */
public class LettuceExceptionConverter implements Converter<Exception, DataAccessException> {

	public DataAccessException convert(Exception ex) {

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
