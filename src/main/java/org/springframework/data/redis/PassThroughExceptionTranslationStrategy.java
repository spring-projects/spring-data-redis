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
import org.springframework.lang.Nullable;

/**
 * {@link PassThroughExceptionTranslationStrategy} returns {@literal null} for unknown {@link Exception}s.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
public class PassThroughExceptionTranslationStrategy implements ExceptionTranslationStrategy {

	private final Converter<Exception, DataAccessException> converter;

	public PassThroughExceptionTranslationStrategy(Converter<Exception, DataAccessException> converter) {
		this.converter = converter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.ExceptionTranslationStrategy#translate(java.lang.Exception)
	 */
	@Nullable
	@Override
	public DataAccessException translate(Exception e) {
		return this.converter.convert(e);
	}

}
