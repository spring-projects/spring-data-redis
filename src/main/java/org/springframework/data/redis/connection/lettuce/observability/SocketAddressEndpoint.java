/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce.observability;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import io.lettuce.core.tracing.Tracing.Endpoint;

/**
 * @author Mark Paluch
 */
record SocketAddressEndpoint(SocketAddress socketAddress) implements Endpoint {

	@Override
	public String toString() {

		if (socketAddress instanceof InetSocketAddress inet) {
			return inet.getHostString() + ":" + inet.getPort();
		}

		return socketAddress.toString();
	}
}
