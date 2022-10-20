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
import java.util.Locale;

import org.springframework.data.redis.connection.lettuce.observability.RedisObservation.HighCardinalityCommandKeyNames;
import org.springframework.data.redis.connection.lettuce.observability.RedisObservation.LowCardinalityCommandKeyNames;

import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.tracing.Tracing.Endpoint;
import io.micrometer.common.KeyValues;

/**
 * Default {@link LettuceObservationConvention} implementation.
 *
 * @author Mark Paluch
 * @since 3.0
 */
record DefaultLettuceObservationConvention(
		boolean includeCommandArgsInSpanTags) implements LettuceObservationConvention {

	@Override
	public KeyValues getLowCardinalityKeyValues(LettuceObservationContext context) {

		Endpoint ep = context.getRequiredEndpoint();
		KeyValues keyValues = KeyValues.of(LowCardinalityCommandKeyNames.DATABASE_SYSTEM.withValue("redis"), //
				LowCardinalityCommandKeyNames.REDIS_COMMAND.withValue(context.getRequiredCommand().getType().name()), //
				LowCardinalityCommandKeyNames.REDIS_SERVER.withValue(ep.toString()));

		if (ep instanceof SocketAddressEndpoint endpoint) {

			if (endpoint.socketAddress()instanceof InetSocketAddress inet) {
				keyValues = keyValues
						.and(KeyValues.of(LowCardinalityCommandKeyNames.NET_PEER_NAME.withValue(inet.getHostString()),
								LowCardinalityCommandKeyNames.NET_PEER_PORT.withValue("" + inet.getPort())));
			} else {
				keyValues = keyValues
						.and(KeyValues.of(LowCardinalityCommandKeyNames.NET_PEER_ADDR.withValue(endpoint.toString())));
			}
		}

		return keyValues;
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(LettuceObservationContext context) {

		RedisCommand<?, ?, ?> command = context.getRequiredCommand();

		if (includeCommandArgsInSpanTags) {

			if (command.getArgs() != null) {
				return KeyValues.of(HighCardinalityCommandKeyNames.STATEMENT
						.withValue(command.getType().name() + " " + command.getArgs().toCommandString()));
			}

			return KeyValues.of(HighCardinalityCommandKeyNames.STATEMENT.withValue(command.getType().name()));
		}

		return KeyValues.empty();
	}

	@Override
	public String getContextualName(LettuceObservationContext context) {
		return context.getRequiredCommand().getType().name().toLowerCase(Locale.ROOT);
	}
}
