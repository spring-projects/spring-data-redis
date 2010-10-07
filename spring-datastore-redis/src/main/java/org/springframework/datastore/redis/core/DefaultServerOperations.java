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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultServerOperations implements ServerOperations {

	protected final Log logger = LogFactory.getLog(getClass());
	
	private RedisOperations redisOperations;
	
	public DefaultServerOperations(RedisOperations redisOperations) {
		this.redisOperations = redisOperations;
	}
	
	public Map<String, String> getServerInfo() {
		return redisOperations.execute(new RedisCallback<Map<String,String>>() {
			public Map<String, String> doInRedis(RedisClient redisClient)
					throws Exception {
				return redisClient.info();				
			}
		});		
	}

}
