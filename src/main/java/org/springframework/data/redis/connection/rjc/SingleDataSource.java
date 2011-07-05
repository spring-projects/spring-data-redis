/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.redis.connection.rjc;

import org.idevlab.rjc.ds.DataSource;
import org.idevlab.rjc.ds.RedisConnection;

/**
 * Basic data source that always returns the same connection.
 * 
 * @author Costin Leau
 */
class SingleDataSource implements DataSource {

	private final RedisConnection connection;

	SingleDataSource(RedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public RedisConnection getConnection() {
		return connection;
	}
}
