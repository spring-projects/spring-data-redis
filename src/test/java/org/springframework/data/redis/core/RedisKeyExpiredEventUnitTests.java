/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Unit tests for {@link RedisKeyExpiredEvent}.
 *
 * @author Mark Paluch
 */
public class RedisKeyExpiredEventUnitTests {

	@Test // DATAREDIS-744
	public void shouldReturnKeyspace() {

		assertThat(new RedisKeyExpiredEvent<Object>("foo".getBytes(), "").getKeyspace(), is(nullValue()));
		assertThat(new RedisKeyExpiredEvent<Object>("foo:bar".getBytes(), "").getKeyspace(), is(equalTo("foo")));
		assertThat(new RedisKeyExpiredEvent<Object>("foo:bar:baz".getBytes(), "").getKeyspace(), is(equalTo("foo")));
	}

	@Test // DATAREDIS-744
	public void shouldReturnId() {

		assertThat(new RedisKeyExpiredEvent<Object>("foo".getBytes(), "").getId(), is(equalTo("foo".getBytes())));
		assertThat(new RedisKeyExpiredEvent<Object>("foo:bar".getBytes(), "").getId(), is(equalTo("bar".getBytes())));
		assertThat(new RedisKeyExpiredEvent<Object>("foo:bar:baz".getBytes(), "").getId(),
				is(equalTo("bar:baz".getBytes())));
	}
}
