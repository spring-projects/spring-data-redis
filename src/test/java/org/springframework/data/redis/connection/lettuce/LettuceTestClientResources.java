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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

import java.util.concurrent.TimeUnit;

/**
 * Client-Resources suitable for testing. Uses {@link TestEventLoopGroupProvider} to preserve the event loop groups
 * between tests. Every time a new {@link LettuceTestClientResources} instance is created, a
 * {@link Runtime#addShutdownHook(Thread) shutdown hook} is added to close the client resources.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceTestClientResources {

	private static final ClientResources SHARED_CLIENT_RESOURCES;

	static {

		SHARED_CLIENT_RESOURCES = DefaultClientResources.builder().eventLoopGroupProvider(new TestEventLoopGroupProvider())
				.build();
		appendShutdownHook();
	}

	private LettuceTestClientResources() {}

	private static void appendShutdownHook() {

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					SHARED_CLIENT_RESOURCES.shutdown(0, 0, TimeUnit.MILLISECONDS).get(1, TimeUnit.SECONDS);
				} catch (Exception o_O) {
					// ignore
				}
			}
		});
	}

	/**
	 * @return the client resources.
	 */
	public static ClientResources getSharedClientResources() {
		return SHARED_CLIENT_RESOURCES;
	}
}
