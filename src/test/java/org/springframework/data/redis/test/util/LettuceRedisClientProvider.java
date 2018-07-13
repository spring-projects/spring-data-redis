/*
 * Copyright 2016-2018 the original author or authors.
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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

import org.junit.rules.ExternalResource;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceRedisClientProvider extends ExternalResource {

	String host = SettingsUtils.getHost();
	int port = SettingsUtils.getPort();

	RedisClient client;

	@Override
	protected void before() {

		try {
			super.before();
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}

		client = RedisClient.create(LettuceTestClientResources.getSharedClientResources(),
				RedisURI.builder().withHost(host).withPort(port).build());
	}

	@Override
	protected void after() {
		super.after();
		client.shutdown();
	}

	public RedisClient getClient() {
		if (client == null) {
			before();
		}
		return client;
	}

	public void destroy() {

		if (client != null) {
			after();
		}
	}

	public static LettuceRedisClientProvider local() {
		return new LettuceRedisClientProvider();
	}
}
