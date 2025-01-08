/*
 * Copyright 2024-2025 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.DefaultJedisClientConfig;

/**
 * Strategy interface for customizing {@link DefaultJedisClientConfig.Builder JedisClientConfig}. Any ClientConfig will
 * be used to call this interface implementation so you can set the protocol, client name, etc. after Spring has applies
 * its defaults.
 *
 * @author Mark Paluch
 * @since 3.4
 * @see redis.clients.jedis.DefaultJedisClientConfig.Builder
 */
@FunctionalInterface
public interface JedisClientConfigBuilderCustomizer {

	/**
	 * Customize the {@link DefaultJedisClientConfig.Builder}.
	 *
	 * @param builder the builder to customize.
	 */
	void customize(DefaultJedisClientConfig.Builder builder);

}
