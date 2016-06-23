/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.redis.test.util;

import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * Enumerates the supported Redis driver types.
 * 
 * @author Thomas Darimont
 */
public enum RedisDriver {

	JEDIS {
		@Override
		public boolean matches(RedisConnectionFactory connectionFactory) {
			return ConnectionUtils.isJedis(connectionFactory);
		}
	},

	LETTUCE {
		@Override
		public boolean matches(RedisConnectionFactory connectionFactory) {
			return ConnectionUtils.isLettuce(connectionFactory);
		}

	};

	/**
	 * @param connectionFactory
	 * @return true of the given {@link RedisConnectionFactory} is supported by the current redis driver.
	 */
	public abstract boolean matches(RedisConnectionFactory connectionFactory);
}
