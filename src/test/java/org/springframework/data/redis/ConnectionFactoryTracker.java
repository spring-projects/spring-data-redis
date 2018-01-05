/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * Basic utility to help with the destruction of {@link RedisConnectionFactory} inside JUnit 4 tests. Simply add the
 * factory during setup and then call {@link #cleanUp()} through the <tt>@AfterClass</tt> method.
 *
 * @author Costin Leau
 * @author Mark Paluch
 */
public abstract class ConnectionFactoryTracker {

	private static Set<Object> connFactories = new LinkedHashSet<>();

	public static void add(RedisConnectionFactory factory) {
		connFactories.add(factory);
	}

	public static void add(Object factory) {
		connFactories.add(factory);
	}

	public static void cleanUp() {
		if (connFactories != null) {
			List<Object> copy = new ArrayList<>(connFactories);
			for (Object connectionFactory : copy) {
				try {
					if (connectionFactory instanceof DisposableBean) {
						((DisposableBean) connectionFactory).destroy();
						// System.out.println("Succesfully cleaned up factory " + connectionFactory);
					}
					connFactories.remove(connectionFactory);
				} catch (Exception ex) {
					System.err.println("Cannot clean factory " + connectionFactory + ex);
				}
			}
		}
	}
}
