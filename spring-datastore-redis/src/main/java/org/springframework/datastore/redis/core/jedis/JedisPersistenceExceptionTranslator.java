/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.core.jedis;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;

/**
 * Translates error messages from Jedis to Spring's Data Access exception class hierarchy
 * 
 * @author Mark Pollack
 *
 */
public class JedisPersistenceExceptionTranslator implements RedisPersistenceExceptionTranslator,
		PersistenceExceptionTranslator {

	public DataAccessException translateException(Exception ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}
}
