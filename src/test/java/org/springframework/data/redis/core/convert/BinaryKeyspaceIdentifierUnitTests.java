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
package org.springframework.data.redis.core.convert;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

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

		assertThat(BinaryKeyspaceIdentifier.isValid(null), is(false));
		assertThat(BinaryKeyspaceIdentifier.isValid("foo".getBytes()), is(false));
		assertThat(BinaryKeyspaceIdentifier.isValid("".getBytes()), is(false));
		assertThat(BinaryKeyspaceIdentifier.isValid("foo:bar".getBytes()), is(true));
		assertThat(BinaryKeyspaceIdentifier.isValid("foo:bar:baz".getBytes()), is(true));
		assertThat(BinaryKeyspaceIdentifier.isValid("foo:bar:baz:phantom".getBytes()), is(true));
	}

	@Test // DATAREDIS-744
	public void shouldReturnKeyspace() {

		assertThat(BinaryKeyspaceIdentifier.of("foo:bar".getBytes()).getKeyspace(), is(equalTo("foo".getBytes())));
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz".getBytes()).getKeyspace(), is(equalTo("foo".getBytes())));
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz:phantom".getBytes()).getKeyspace(),
				is(equalTo("foo".getBytes())));
	}

	@Test // DATAREDIS-744
	public void shouldReturnId() {

		assertThat(BinaryKeyspaceIdentifier.of("foo:bar".getBytes()).getId(), is(equalTo("bar".getBytes())));
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz".getBytes()).getId(), is(equalTo("bar:baz".getBytes())));
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz:phantom".getBytes()).getId(),
				is(equalTo("bar:baz".getBytes())));
	}

	@Test // DATAREDIS-744
	public void shouldReturnPhantomKey() {

		assertThat(BinaryKeyspaceIdentifier.of("foo:bar".getBytes()).isPhantomKey(), is(false));
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz".getBytes()).isPhantomKey(), is(false));
		assertThat(BinaryKeyspaceIdentifier.of("foo:bar:baz:phantom".getBytes()).isPhantomKey(), is(true));
	}
}
