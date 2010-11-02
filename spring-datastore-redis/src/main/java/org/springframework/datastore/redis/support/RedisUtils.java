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

package org.springframework.datastore.redis.support;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.datastore.redis.core.RedisClient;

/**
 * Generic utility methods for working with Redis. Mainly for internal use
 * within the framework.
 * @author Mark Pollack
 *
 */
public class RedisUtils {


	private static final Log logger = LogFactory.getLog(RedisUtils.class);

	
	/**
	 * Close the given Redis Client and ignore any thrown exception.
	 * This is useful for typical <code>finally</code> blocks in manual Redis code.
	 * @param channel the RabbitMQ Channel to close (may be <code>null</code>)
	 */
	public static void closeClient(RedisClient redisClient) {
		if (redisClient != null) {
			try {
				redisClient.disconnect();
			}
			catch (IOException ex) {
				logger.debug("Could not close Redis Channel", ex);
			}
			catch (Throwable ex) {
				logger.debug("Unexpected exception on closing Redis Client", ex);
			}
		}
	}
}
