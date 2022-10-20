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

import org.springframework.lang.Nullable;

import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.tracing.Tracing.Endpoint;
import io.micrometer.observation.Observation;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;

/**
 * Micrometer {@link Observation.Context} holding Lettuce contextual details.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class LettuceObservationContext extends SenderContext<Object> {

	private volatile @Nullable RedisCommand<?, ?, ?> command;

	private volatile @Nullable Endpoint endpoint;

	public LettuceObservationContext(String serviceName) {
		super((carrier, key, value) -> {}, Kind.CLIENT);
		setRemoteServiceName(serviceName);
	}

	public RedisCommand<?, ?, ?> getRequiredCommand() {

		RedisCommand<?, ?, ?> local = command;

		if (local == null) {
			throw new IllegalArgumentException("LettuceObservationContext is not associated with a Command");
		}

		return local;
	}

	public void setCommand(RedisCommand<?, ?, ?> command) {
		this.command = command;
	}

	public Endpoint getRequiredEndpoint() {

		Endpoint local = endpoint;

		if (local == null) {
			throw new IllegalArgumentException("LettuceObservationContext is not associated with a Endpoint");
		}

		return local;
	}

	public void setEndpoint(Endpoint endpoint) {
		this.endpoint = endpoint;
	}
}
