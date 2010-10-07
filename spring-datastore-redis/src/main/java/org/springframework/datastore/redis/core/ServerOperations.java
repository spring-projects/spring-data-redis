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

import java.util.Map;

/**
 * Server operations for Redis
 * 
 * @author Mark Pollack
 *
 */
public interface ServerOperations {

	
	// Connection handling
	
	
	
	
	/**
	 * Calls the Redis 'info' command that returns different information and statistics about the server.
	 * The reply is parsed into a Map for easy programmatic access. 
	 * Corresponds to the Redis  InfoCommand INFO
	 * @see <a href="http://code.google.com/p/redis/wiki/InfoCommand">InfoCommand</a>
	 * @return
	 */
	Map<String,String> getServerInfo();
	
	// TODO Commands Monitor, SlaveOf, Config
}
