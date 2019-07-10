/*
 * Copyright 2018-2019 the original author or authors.
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

import org.springframework.data.redis.core.convert.MappingRedisConverter.BinaryKeyspaceIdentifier;

/**
 * Unit tests for {@link BinaryKeyspaceIdentifier}.
 *
 * @author Mark Paluch
 */
public class BinaryKeyspaceIdentifierUnitTests {

	@Test // DATAREDIS-744
	public void shouldReturnIfKeyIsValid() {

		assertThat(BinaryKeyspaceIdentifier.isValid(null)).isFalse();
		assertThat(BinaryKeyspaceIdentifier.isValid("foo".getBytes())).isFalse();
		assertThat(BinaryKeyspaceIdentifier.isValid("".getBytes())).isFalse();
		assertThat(BinaryKeyspaceIdentifier.isValid("foo:bar".getBytes())).isTrue();
		assertThat(BinaryKeyspaceIdentifier.isValid("foo:bar:baz".getBytes())).isTrue();
		assertThat(BinaryKeyspaceIdentifier.isValid("foo:bar:baz:phantom".getBytes())).isTrue();
	}

	@Test // DATAREDIS-744
	public void shouldReturnKeyspace() {

		assertThat(BinaryKeyspaceIdentifier.of("foo:bar".getBytes()).getKeyspace()).isEqualTo("foo".getBytes());
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz".getBytes()).getKeyspace()).isEqualTo("foo".getBytes());
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz:phantom".getBytes()).getKeyspace()).isEqualTo("foo".getBytes());
	}

	@Test // DATAREDIS-744
	public void shouldReturnId() {

		assertThat(BinaryKeyspaceIdentifier.of("foo:bar".getBytes()).getId()).isEqualTo("bar".getBytes());
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz".getBytes()).getId()).isEqualTo("bar:baz".getBytes());
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz:phantom".getBytes()).getId()).isEqualTo("bar:baz".getBytes());
	}

	@Test // DATAREDIS-744
	public void shouldReturnPhantomKey() {

		assertThat(BinaryKeyspaceIdentifier.of("foo:bar".getBytes()).isPhantomKey()).isFalse();
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz".getBytes()).isPhantomKey()).isFalse();
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz:phantom".getBytes()).isPhantomKey()).isTrue();
	}
}
