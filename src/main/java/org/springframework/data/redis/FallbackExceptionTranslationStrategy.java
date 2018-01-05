/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;

/**
 * {@link FallbackExceptionTranslationStrategy} returns {@link RedisSystemException} for unknown {@link Exception}s.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.4
 */
public class FallbackExceptionTranslationStrategy extends PassThroughExceptionTranslationStrategy {

	public FallbackExceptionTranslationStrategy(Converter<Exception, DataAccessException> converter) {
		super(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.PassThroughExceptionTranslationStrategy#translate(java.lang.Exception)
	 */
	@Override
	public DataAccessException translate(Exception e) {

		DataAccessException translated = super.translate(e);
		return translated != null ? translated : getFallback(e);
	}

	/**
	 * Returns a new {@link RedisSystemException} wrapping the given {@link Exception}.
	 *
	 * @param e causing exception.
	 * @return the fallback exception.
	 */
	protected RedisSystemException getFallback(Exception e) {
		return new RedisSystemException("Unknown redis exception", e);
	}

}
