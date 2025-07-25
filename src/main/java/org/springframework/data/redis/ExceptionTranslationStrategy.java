/*
 * Copyright 2014-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;

/**
 * Potentially translates an {@link Exception} into appropriate {@link DataAccessException}.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @since 1.4
 */
public interface ExceptionTranslationStrategy {

	/**
	 * Potentially translate the given {@link Exception} into {@link DataAccessException}.
	 *
	 * @param e must not be {@literal null}.
	 * @return can be {@literal null} if given {@link Exception} cannot be translated.
	 */
	@Nullable
	DataAccessException translate(Exception e);

}
