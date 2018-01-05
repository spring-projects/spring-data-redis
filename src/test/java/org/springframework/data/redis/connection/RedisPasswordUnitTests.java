/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

/**
 * Unit tests for {@link RedisPassword}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class RedisPasswordUnitTests {

	@Test // DATAREDIS-574
	public void shouldCreateFromEmptyString() {
		assertThat(RedisPassword.of("").toOptional()).isEmpty();
	}

	@Test // DATAREDIS-574
	public void shouldCreateFromExistingString() {
		assertThat(RedisPassword.of("foo").map(String::new)).contains("foo");
	}

	@Test // DATAREDIS-574
	public void shouldCreateFromEmptyCharArray() {
		assertThat(RedisPassword.of("".toCharArray()).toOptional()).isEmpty();
	}

	@Test // DATAREDIS-574
	public void shouldCreateFromExistingCharArray() {
		assertThat(RedisPassword.of("foo".toCharArray()).map(String::new)).contains("foo");
	}

	@Test // DATAREDIS-574
	public void toStringShouldHideValue() {
		assertThat(RedisPassword.of("foo".toCharArray()).toString()).startsWith("RedisPassword[**").doesNotContain("foo");
	}
}
