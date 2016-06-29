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

package org.springframework.data.redis.connection;

import java.nio.ByteBuffer;
import java.util.List;

import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveClusterKeyCommands extends ReactiveKeyCommands {

	/**
	 * Retrieve all {@literal keys} for a given {@literal pattern} from {@link RedisNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @param pattern must not be {@literal null}.
	 * @return
	 */
	Mono<List<ByteBuffer>> keys(RedisClusterNode node, ByteBuffer pattern);

	/**
	 * Retrieve a random {@literal key} from {@link RedisNode}.
	 *
	 * @param node must not be {@literal null}.
	 * @return
	 */
	Mono<ByteBuffer> randomKey(RedisClusterNode node);

}
