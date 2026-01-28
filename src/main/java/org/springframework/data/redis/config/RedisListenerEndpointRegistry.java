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

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates the necessary {@link RedisMessageListenerContainer} instances for the registered {@link RedisListenerEndpoint
 * endpoints}.
 * <p>
 * Manages the lifecycle of the listener containers.
 *
 * @author Ilyass Bougati
 */
public class RedisListenerEndpointRegistry implements DisposableBean, SmartLifecycle {

	private final Map<String, RedisMessageListenerContainer> listenerContainers = new ConcurrentHashMap<>();
	private boolean running;

	public void registerListenerContainer(RedisListenerEndpoint endpoint, RedisListenerContainerFactory<?> factory) {
		RedisMessageListenerContainer container = factory.createListenerContainer(endpoint);
		this.listenerContainers.put(endpoint.getId(), container);

		if (this.running) {
			container.start();
		}
	}

	@Override
	public void start() {
		this.running = true;
		for (RedisMessageListenerContainer container : this.listenerContainers.values()) {
			container.start();
		}
	}

	@Override
	public void stop() {
		this.running = false;
		for (RedisMessageListenerContainer container : this.listenerContainers.values()) {
			container.stop();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public void destroy() throws Exception {
		for (RedisMessageListenerContainer container : this.listenerContainers.values()) {
			container.destroy();
		}
	}

	public Map<String, RedisMessageListenerContainer> getListenerContainers() {
		return listenerContainers;
	}
}
