/*
 * Copyright 2020-2025 the original author or authors.
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

import redis.clients.jedis.util.IOUtils;

import java.io.Closeable;
import java.util.LinkedList;

import org.springframework.core.OrderComparator;

/**
 * Shutdown queue allowing ordered resource shutdown (LIFO).
 *
 * @author Mark Paluch
 */
public enum ShutdownQueue {

	INSTANCE;

	static {

		Runtime.getRuntime().addShutdownHook(new Thread(ShutdownQueue::close));
	}

	private static void close() {

		Closeable closeable;

		OrderComparator.sort(INSTANCE.closeables);

		while ((closeable = INSTANCE.closeables.pollLast()) != null) {
			try {
				closeable.close();
			} catch (Exception o_O) {
				// ignore
				o_O.printStackTrace();
			}
		}
	}

	private final LinkedList<Closeable> closeables = new LinkedList<>();

	public static void register(ShutdownCloseable closeable) {
		INSTANCE.closeables.add(closeable::close);
	}

	public static void register(Closeable closeable) {
		INSTANCE.closeables.add(closeable);
	}

	public static void register(AutoCloseable closeable) {
		INSTANCE.closeables.add(() -> IOUtils.closeQuietly(closeable));
	}

	public interface ShutdownCloseable {

		void close();
	}

}
