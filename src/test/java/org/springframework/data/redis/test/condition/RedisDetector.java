/*
 * Copyright 2020-2022 the original author or authors.
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
package org.springframework.data.redis.test.condition;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.util.Lazy;

/**
 * Utility to detect Redis operation modes.
 *
 * @author Mark Paluch
 */
public class RedisDetector {

	private static final Lazy<Boolean> CLUSTER_AVAILABLE = Lazy.of(() -> {

		try (JedisCluster cluster = new JedisCluster(
				new HostAndPort(SettingsUtils.getHost(), SettingsUtils.getClusterPort()))) {
			cluster.getConnectionFromSlot(1).close();

			return true;
		} catch (Exception e) {
			return false;
		}
	});

	public static boolean isClusterAvailable() {
		return CLUSTER_AVAILABLE.get();
	}

	public static boolean canConnectToPort(int port) {

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(SettingsUtils.getHost(), port), 100);

			return true;
		} catch (IOException e) {
			return false;
		}
	}

}
