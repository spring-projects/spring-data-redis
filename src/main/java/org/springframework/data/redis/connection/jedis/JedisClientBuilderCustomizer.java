/*
 * Copyright 2026-present the original author or authors.
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

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.builders.AbstractClientBuilder;

/**
 * Strategy interface for customizing {@link AbstractClientBuilder ClientBuilder}. Objects implementing this interface
 * will be invoked with any ClientBuilder so you can customize the client builder after Spring has applied its defaults
 * when using {@link RedisClient} and {@link RedisClusterClient} etc. The builder is <b>not</b> applied using
 * {@link redis.clients.jedis.Jedis} and {@link redis.clients.jedis.JedisCluster}.
 * <p>
 * {@link AbstractClientBuilder} is a generic builder that can be used to build any kind of client. Depending on the
 * intended client to be built, implementations may check the builder type and cast it accordingly for further
 * customization.
 *
 * @author Mark Paluch
 * @since 4.1
 */
@FunctionalInterface
public interface JedisClientBuilderCustomizer {

	/**
	 * Customize the {@link AbstractClientBuilder}.
	 *
	 * @param builder the builder to customize.
	 */
	void customize(AbstractClientBuilder<?, ?> builder);

}
