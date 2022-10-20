/*
 * Copyright 2013-2022 the original author or authors.
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

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * A Redis-based {@link io.micrometer.observation.Observation}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
enum RedisObservation implements ObservationDocumentation {

	/**
	 * Timer created around a Redis command execution.
	 */
	REDIS_COMMAND_OBSERVATION {

		@Override
		public String getName() {
			return "spring.data.redis";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return LowCardinalityCommandKeyNames.values();
		}

		@Override
		public KeyName[] getHighCardinalityKeyNames() {
			return HighCardinalityCommandKeyNames.values();
		}
	};

	/**
	 * Enums related to low cardinality key names for Redis commands.
	 */
	enum LowCardinalityCommandKeyNames implements KeyName {

		/**
		 * Database system.
		 */
		DATABASE_SYSTEM {
			@Override
			public String asString() {
				return "db.system";
			}
		},

		/**
		 * Name of the database host.
		 */
		NET_PEER_NAME {
			@Override
			public String asString() {
				return "net.peer.name";
			}
		},

		/**
		 * Logical remote port number.
		 */
		NET_PEER_PORT {
			@Override
			public String asString() {
				return "net.peer.port";
			}
		},

		/**
		 * Redis peer address.
		 */
		NET_PEER_ADDR {
			@Override
			public String asString() {
				return "net.sock.peer.addr";
			}
		},

		/**
		 * Redis endpoint.
		 */
		REDIS_SERVER {
			@Override
			public String asString() {
				return "spring.data.redis.server";
			}
		},

		/**
		 * Redis command value.
		 */
		REDIS_COMMAND {
			@Override
			public String asString() {
				return "db.operation";
			}
		}

	}

	/**
	 * Enums related to high cardinality key names for Redis commands.
	 */
	enum HighCardinalityCommandKeyNames implements KeyName {

		/**
		 * Redis statement.
		 */
		STATEMENT {
			@Override
			public String asString() {
				return "db.statement";
			}
		},

		/**
		 * Redis error response.
		 */
		ERROR {
			@Override
			public String asString() {
				return "spring.data.redis.command.error";
			}
		}
	}
}
