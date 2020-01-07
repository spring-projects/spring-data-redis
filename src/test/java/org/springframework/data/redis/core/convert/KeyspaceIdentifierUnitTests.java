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
package org.springframework.data.redis.core.convert;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

import org.springframework.data.redis.core.convert.MappingRedisConverter.KeyspaceIdentifier;

/**
 * Unit tests for {@link KeyspaceIdentifier}.
 *
 * @author Mark Paluch
 */
public class KeyspaceIdentifierUnitTests {

	@Test // DATAREDIS-744
	public void shouldReturnIfKeyIsValid() {

		assertThat(KeyspaceIdentifier.isValid(null)).isFalse();
		assertThat(KeyspaceIdentifier.isValid("foo")).isFalse();
		assertThat(KeyspaceIdentifier.isValid("")).isFalse();
		assertThat(KeyspaceIdentifier.isValid("foo:bar")).isTrue();
		assertThat(KeyspaceIdentifier.isValid("foo:bar:baz")).isTrue();
		assertThat(KeyspaceIdentifier.isValid("foo:bar:baz:phantom")).isTrue();
	}

	@Test // DATAREDIS-744
	public void shouldReturnKeyspace() {

		assertThat(KeyspaceIdentifier.of("foo:bar").getKeyspace()).isEqualTo("foo");
		assertThat(KeyspaceIdentifier.of("foo:bar:baz").getKeyspace()).isEqualTo("foo");
		assertThat(KeyspaceIdentifier.of("foo:bar:baz:phantom").getKeyspace()).isEqualTo("foo");
	}

	@Test // DATAREDIS-744
	public void shouldReturnId() {

		assertThat(KeyspaceIdentifier.of("foo:bar").getId()).isEqualTo("bar");
		assertThat(KeyspaceIdentifier.of("foo:bar:baz").getId()).isEqualTo("bar:baz");
		assertThat(KeyspaceIdentifier.of("foo:bar:baz:phantom").getId()).isEqualTo("bar:baz");
	}

	@Test // DATAREDIS-744
	public void shouldReturnPhantomKey() {

		assertThat(KeyspaceIdentifier.of("foo:bar").isPhantomKey()).isFalse();
		assertThat(KeyspaceIdentifier.of("foo:bar:baz").isPhantomKey()).isFalse();
		assertThat(KeyspaceIdentifier.of("foo:bar:baz:phantom").isPhantomKey()).isTrue();
	}
}
