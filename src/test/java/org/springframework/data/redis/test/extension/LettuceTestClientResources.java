/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.redis.test.extension;

import io.lettuce.core.event.Event;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import reactor.core.publisher.Flux;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.springframework.core.Ordered;

/**
 * Client-Resources suitable for testing. Every time a new {@link LettuceTestClientResources} instance is created, a
 * {@link Runtime#addShutdownHook(Thread) shutdown hook} is added to close the client resources.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class LettuceTestClientResources {

	private static final ClientResources SHARED_CLIENT_RESOURCES;

	static {

		SHARED_CLIENT_RESOURCES = newClientResources();
		ShutdownQueue.register(new SharedClientResources(SHARED_CLIENT_RESOURCES));
	}

	private static ClientResources newClientResources() {

		return DefaultClientResources.builder().ioThreadPoolSize(4).computationThreadPoolSize(4).eventBus(new EventBus() {
			@Override
			public Flux<Event> get() {
				return Flux.empty();
			}

			@Override
			public void publish(Event event) {

			}
		}).build();
	}

	private LettuceTestClientResources() {}

	/**
	 * @return the client resources.
	 */
	public static ClientResources getSharedClientResources() {
		return SHARED_CLIENT_RESOURCES;
	}

	static class SharedClientResources implements Ordered, Closeable {
		private final ClientResources clientResources;

		public SharedClientResources(ClientResources clientResources) {
			this.clientResources = clientResources;
		}

		@Override
		public void close() throws IOException {
			clientResources.shutdown(0, 0, TimeUnit.MILLISECONDS);
		}

		@Override
		public int getOrder() {
			return HIGHEST_PRECEDENCE;
		}
	}
}
