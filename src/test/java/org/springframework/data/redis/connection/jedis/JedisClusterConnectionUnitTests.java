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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.args.Rawable;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.ArgumentCaptor;

import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.PassThroughExceptionTranslationStrategy;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.ClusterNodeResourceProvider;
import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;

/**
 * Unit tests for {@link JedisClusterConnection}.
 *
 * @author Moritz Halbritter
 */
class JedisClusterConnectionUnitTests {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new PassThroughExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private final UnifiedJedis nodeClient = mock(UnifiedJedis.class);

	private JedisClusterConnection connection;

	@BeforeEach
	void setUp() {

		ClusterTopologyProvider topologyProvider = mock(ClusterTopologyProvider.class);
		when(topologyProvider.getTopology())
				.thenReturn(new ClusterTopology(Set.of(CLUSTER_NODE_1, CLUSTER_NODE_2, CLUSTER_NODE_3)));

		ClusterNodeResourceProvider resourceProvider = mock(ClusterNodeResourceProvider.class);
		when(resourceProvider.getResourceForSpecificNode(any())).thenReturn(nodeClient);

		ClusterCommandExecutor executor = new ClusterCommandExecutor(topologyProvider, resourceProvider,
				EXCEPTION_TRANSLATION);

		connection = new JedisClusterConnection(mock(UnifiedJedis.class), executor, topologyProvider);
	}

	@ParameterizedTest // GH-3497
	@NullSource
	@EnumSource(MigrateOption.class)
	void migrateShouldSendDestinationDatabase(MigrateOption option) {

		connection.migrate("foo".getBytes(), CLUSTER_NODE_2, 5, option, 1500);

		String expectedOption = option != null ? " " + option.name() : "";
		assertThat(captureCommand())
				.isEqualTo("MIGRATE " + CLUSTER_HOST + " " + MASTER_NODE_2_PORT + "  5 1500" + expectedOption + " KEYS foo");
	}

	private String captureCommand() {

		ArgumentCaptor<CommandArguments> captor = ArgumentCaptor.forClass(CommandArguments.class);
		verify(nodeClient).executeCommand(captor.capture());

		return StreamSupport.stream(captor.getValue().spliterator(), false).map(Rawable::getRaw)
				.map(raw -> new String(raw, StandardCharsets.UTF_8)).collect(Collectors.joining(" "));
	}

}
