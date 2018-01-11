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
import org.springframework.data.redis.core.convert.MappingRedisConverter.KeyspaceIdentifier;

/**
 * Unit tests for {@link KeyspaceIdentifier}.
 *
 * @author Mark Paluch
 */
public class KeyspaceIdentifierUnitTests {

	@Test // DATAREDIS-744
	public void shouldReturnIfKeyIsValid() {

		assertThat(KeyspaceIdentifier.isValid(null), is(false));
		assertThat(KeyspaceIdentifier.isValid("foo"), is(false));
		assertThat(KeyspaceIdentifier.isValid(""), is(false));
		assertThat(KeyspaceIdentifier.isValid("foo:bar"), is(true));
		assertThat(KeyspaceIdentifier.isValid("foo:bar:baz"), is(true));
		assertThat(KeyspaceIdentifier.isValid("foo:bar:baz:phantom"), is(true));
	}

	@Test // DATAREDIS-744
	public void shouldReturnKeyspace() {

		assertThat(KeyspaceIdentifier.of("foo:bar").getKeyspace(), is("foo"));
		assertThat(KeyspaceIdentifier.of("foo:bar:baz").getKeyspace(), is("foo"));
		assertThat(KeyspaceIdentifier.of("foo:bar:baz:phantom").getKeyspace(), is("foo"));
	}

	@Test // DATAREDIS-744
	public void shouldReturnId() {

		assertThat(KeyspaceIdentifier.of("foo:bar").getId(), is("bar"));
		assertThat(KeyspaceIdentifier.of("foo:bar:baz").getId(), is("bar:baz"));
		assertThat(KeyspaceIdentifier.of("foo:bar:baz:phantom").getId(), is("bar:baz"));
	}

	@Test // DATAREDIS-744
	public void shouldReturnPhantomKey() {

		assertThat(KeyspaceIdentifier.of("foo:bar").isPhantomKey(), is(false));
		assertThat(KeyspaceIdentifier.of("foo:bar:baz").isPhantomKey(), is(false));
		assertThat(KeyspaceIdentifier.of("foo:bar:baz:phantom").isPhantomKey(), is(true));
	}
}
