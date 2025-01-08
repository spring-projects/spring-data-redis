/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.redis.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author Christoph Strobl
 */
public class ConnectionVerifier<T extends RedisConnectionFactory> {

	private final T connectionFactory;
	private final List<Consumer<RedisConnection>> steps = new ArrayList<>(3);

	private Consumer<T> initFactoryFunction = this::initializeFactoryIfRequired;

	ConnectionVerifier(T connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public static <V extends RedisConnectionFactory> ConnectionVerifier<V> create(V connectionFactory) {
		return new ConnectionVerifier<>(connectionFactory);
	}

	public ConnectionVerifier<T> initializeFactory(Consumer<T> initFunction) {

		this.initFactoryFunction = initFunction;
		return this;
	}

	public ConnectionVerifier<T> execute(Consumer<RedisConnection> connectionConsumer) {
		this.steps.add(connectionConsumer);
		return this;
	}

	public void verify() {
		verifyAndRun(it -> {});
	}

	public void verifyAndClose() {
		verifyAndRun(this::disposeFactoryIfNeeded);
	}

	public void verifyAndRun(Consumer<T> disposeFunction) {

		initFactoryFunction.accept(connectionFactory);

		try (RedisConnection connection = connectionFactory.getConnection()) {
			steps.forEach(step -> step.accept(connection));
		} finally {
			disposeFunction.accept(connectionFactory);
		}
	}

	private void initializeFactoryIfRequired(T factory) {

		if (factory instanceof InitializingBean initializingBean) {
			try {
				initializingBean.afterPropertiesSet();
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		if (factory instanceof SmartLifecycle smartLifecycle) {
			if (smartLifecycle.isAutoStartup() && !smartLifecycle.isRunning()) {
				smartLifecycle.start();
			}
		}
	}

	private void disposeFactoryIfNeeded(T it) {

		if (it instanceof DisposableBean bean) {
			try {
				bean.destroy();
			} catch (Exception ex) {
				throw new DataAccessResourceFailureException("Cannot close resource", ex);
			}
		} else if (it instanceof Closeable closeable) {
			try {
				closeable.close();
			} catch (IOException ex) {
				throw new DataAccessResourceFailureException("Cannot close resource", ex);
			}
		} else if (it instanceof SmartLifecycle smartLifecycle && smartLifecycle.isRunning()) {
			smartLifecycle.stop();
		}
	}
}
