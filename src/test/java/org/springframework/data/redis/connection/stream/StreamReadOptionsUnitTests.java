/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.connection.stream;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;

import org.junit.Test;

/**
 * Unit tests for {@link StreamReadOptions}.
 * 
 * @author Mark Paluch
 */
public class StreamReadOptionsUnitTests {

	@Test // DATAREDIS-1138
	public void shouldConsiderBlocking() {

		assertThat(StreamReadOptions.empty().isBlocking()).isFalse();
		assertThat(StreamReadOptions.empty().block(Duration.ofSeconds(1)).isBlocking()).isTrue();
		assertThat(StreamReadOptions.empty().block(Duration.ZERO).isBlocking()).isTrue();
	}
}
