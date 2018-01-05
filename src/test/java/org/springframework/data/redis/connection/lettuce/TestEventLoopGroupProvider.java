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

import io.lettuce.core.resource.DefaultEventLoopGroupProvider;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.TimeUnit;

/**
 * A {@link io.lettuce.core.resource.EventLoopGroupProvider} suitable for testing. Preserves the event loop groups
 * between tests. Every time a new {@link TestEventLoopGroupProvider} instance is created, a
 * {@link Runtime#addShutdownHook(Thread) shutdown hook} is added to close the resources.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class TestEventLoopGroupProvider extends DefaultEventLoopGroupProvider {

	private static final int NUMBER_OF_THREADS = 10;

	public TestEventLoopGroupProvider() {

		super(NUMBER_OF_THREADS);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					TestEventLoopGroupProvider.this.shutdown(0, 0, TimeUnit.MILLISECONDS).get(1, TimeUnit.SECONDS);
				} catch (Exception o_O) {
					// ignored
				}
			}
		});
	}

	@Override
	public Promise<Boolean> release(EventExecutorGroup eventLoopGroup, long quietPeriod, long timeout, TimeUnit unit) {

		DefaultPromise<Boolean> result = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
		result.setSuccess(true);

		return result;
	}
}
