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

package org.springframework.datastore.redis.core;

import java.util.List;

import org.springframework.dao.DataAccessException;

/**
 * Interface specifying a set of Redis operations.
 * Implemented by {@link RedisTemplate}.
 * 
 * @author Mark Pollack
 *
 */
public interface RedisOperations extends KeyValueOperations {

	<T> T execute(RedisCallback<T> action) throws DataAccessException;
	
	ServerOperations getServerOperations();
	
	ListOperations getListOperations();
	
	SetOperations getSetOperations();
	

	
}
