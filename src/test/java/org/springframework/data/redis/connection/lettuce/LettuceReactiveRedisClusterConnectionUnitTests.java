/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider.TargetAware;

/**
 * Unit tests for {@link LettuceReactiveRedisClusterConnection}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class LettuceReactiveRedisClusterConnectionUnitTests {

	static final RedisClusterNode NODE1 = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);

	@Mock StatefulRedisClusterConnection<ByteBuffer, ByteBuffer> sharedConnection;
	@Mock StatefulRedisConnection<ByteBuffer, ByteBuffer> nodeConnection;

	@Mock RedisClusterClient clusterClient;
	@Mock RedisAdvancedClusterReactiveCommands<ByteBuffer, ByteBuffer> reactiveCommands;
	@Mock RedisReactiveCommands<ByteBuffer, ByteBuffer> reactiveNodeCommands;
	@Mock(extraInterfaces = TargetAware.class) LettuceConnectionProvider connectionProvider;

	@Before
	public void before() {

		when(connectionProvider.getConnectionAsync(any())).thenReturn(CompletableFuture.completedFuture(sharedConnection));
		when(sharedConnection.getConnectionAsync(anyString(), anyInt())).thenReturn(CompletableFuture.completedFuture(nodeConnection));
		when(nodeConnection.reactive()).thenReturn(reactiveNodeCommands);
	}

	@Test // DATAREDIS-659, DATAREDIS-708
	public void bgReWriteAofShouldRespondCorrectly() {

		LettuceReactiveRedisClusterConnection connection = new LettuceReactiveRedisClusterConnection(connectionProvider,
				clusterClient);

		when(reactiveNodeCommands.bgrewriteaof()).thenReturn(Mono.just("OK"));

		connection.serverCommands().bgReWriteAof(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAREDIS-659, DATAREDIS-708
	public void bgSaveShouldRespondCorrectly() {

		LettuceReactiveRedisClusterConnection connection = new LettuceReactiveRedisClusterConnection(connectionProvider,
				clusterClient);

		when(reactiveNodeCommands.bgsave()).thenReturn(Mono.just("OK"));

		connection.serverCommands().bgSave(NODE1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}
}
