/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.datastore.redis.support;

import org.springframework.dao.DataAccessException;

/**
 * Interface implemented by Spring integrations with Redis for drivers
 * that throw runtime and checked exceptions.
 * 
 * @author Mark Pollack
 *
 */
public interface RedisPersistenceExceptionTranslator {

	
	//NOTE some client libraries throw checked exceptions.
	DataAccessException translateException(Exception ex);
}
