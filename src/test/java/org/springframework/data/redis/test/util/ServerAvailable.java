/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.test.util;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Christoph Strobl
 */
public class ServerAvailable implements TestRule {

	private final String host;
	private final int port;

	ServerAvailable(String host, int port) {

		this.host = host;
		this.port = port;
	}

	public static ServerAvailable runningAtLocalhost() {
		return runningAtLocalhost(6379);
	}

	public static ServerAvailable runningAtLocalhost(int port) {
		return runningAt("localhost", port);
	}

	public static ServerAvailable runningAt(String host, int port) {
		return new ServerAvailable(host, port);
	}

	@Override
	public Statement apply(Statement base, Description description) {

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				if (!isAvailable()) {

					throw new AssumptionViolatedException(
							String.format("Redis Server (%s:%s) did not answer. Is it up and running?", host, port));
				}

				base.evaluate();
			}
		};
	}

	public boolean isAvailable() {

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(host, port), 100);
			return true;
		} catch (Exception e) {
			return false;
		}

	}
}
