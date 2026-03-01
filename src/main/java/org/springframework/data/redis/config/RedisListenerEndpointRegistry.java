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
package org.springframework.data.redis.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Creates the necessary {@link RedisMessageListenerContainer} instances for the registered {@link RedisListenerEndpoint
 * endpoints}.
 * <p>
 * Manages the lifecycle of the listener containers.
 *
 * @author Ilyass Bougati
 * @since 4.1
 */
public class RedisListenerEndpointRegistry implements SmartLifecycle {

	private final List<RedisListenerEndpoint> endpoints = new ArrayList<>();

	private boolean running;

	/**
	 * Register a new {@link RedisListenerEndpoint} with the given {@link RedisMessageListenerContainer}.
	 *
	 * @param endpoint the endpoint to register.
	 * @param container the container to register the endpoint with.
	 */
	public void registerListener(RedisListenerEndpoint endpoint, RedisMessageListenerContainer container) {
		endpoint.register(container);

		synchronized (this.endpoints) {
			this.endpoints.add(endpoint);
		}
	}

	@Override
	public void start() {

		for (RedisListenerEndpoint endpoint : this.endpoints) {
			if (endpoint instanceof Lifecycle && !((SmartLifecycle) endpoint).isRunning()) {
				((Lifecycle) endpoint).start();
			}
		}

		this.running = true;
	}

	@Override
	public void stop() {
		for (RedisListenerEndpoint endpoint : this.endpoints) {
			if (endpoint instanceof Lifecycle && ((SmartLifecycle) endpoint).isRunning()) {
				((Lifecycle) endpoint).stop();
			}
		}
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	public List<RedisListenerEndpoint> getEndpoints() {
		return endpoints;
	}
}
