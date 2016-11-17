/*
 * Copyright 2016. the original author or authors.
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

package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.connection.RedisClusterNode.*;
import static org.springframework.data.redis.connection.lettuce.LettuceReactiveCommandsTestsBase.*;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;
import org.springframework.data.redis.connection.RedisClusterNode;

import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveClusterKeyCommandsTests extends LettuceReactiveClusterCommandsTestsBase {

	static final RedisClusterNode NODE_1 = newRedisClusterNode().listeningAt("127.0.0.1", 7379).build();

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void keysShouldReturnOnlyKeysFromSelectedNode() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		List<ByteBuffer> result = connection.keyCommands().keys(NODE_1, ByteBuffer.wrap("*".getBytes())).block();
		assertThat(result, hasSize(1));
		assertThat(result, contains(KEY_1_BBUFFER));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void randomkeyShouldReturnOnlyKeysFromSelectedNode() {

		nativeCommands.set(KEY_1, VALUE_1);
		nativeCommands.set(KEY_2, VALUE_2);

		Mono<ByteBuffer> randomkey = connection.keyCommands().randomKey(NODE_1);

		for (int i = 0; i < 10; i++) {
			assertThat(randomkey.block(), is(equalTo(KEY_1_BBUFFER)));
		}
	}

}
