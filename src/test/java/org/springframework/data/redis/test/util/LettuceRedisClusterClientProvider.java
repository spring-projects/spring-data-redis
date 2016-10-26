/*
 * Copyright 2016. the original author or authors.
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

/*
 * Copyright 2016 the original author or authors.
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

import org.junit.rules.ExternalResource;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;

import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class LettuceRedisClusterClientProvider extends ExternalResource {

	String host = "127.0.0.1";
	int port = 7379;

	RedisClusterClient client;

	@Override
	protected void before() {

		try {
			super.before();
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}

		client = RedisClusterClient.create(LettuceTestClientResources.getSharedClientResources(),
				RedisURI.builder().withHost(host).withPort(port).build());
	}

	@Override
	protected void after() {

		super.after();
		client.shutdown();
	}

	public RedisClusterClient getClient() {

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

	public boolean test() {

		try {
			StatefulRedisClusterConnection<String, String> conn = getClient().connect();
			conn.close();
		} catch (Exception e) {
			return false;
		}

		return true;
	}

	public static LettuceRedisClusterClientProvider local() {
		return new LettuceRedisClusterClientProvider();
	}
}
