/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core.mapping;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.mapping.RedisMappingContext.ConfigAwareKeySpaceResolver;

/**
 * @author Christoph Strobl
 */
public class ConfigAwareKeySpaceResolverUnitTests {

	static final String CUSTOM_KEYSPACE = "car'a'carn";
	KeyspaceConfiguration config = new KeyspaceConfiguration();
	ConfigAwareKeySpaceResolver resolver;

	@Before
	public void setUp() {
		this.resolver = new ConfigAwareKeySpaceResolver(config);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test(expected = IllegalArgumentException.class)
	public void resolveShouldThrowExceptionWhenTypeIsNull() {
		resolver.resolveKeySpace(null);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveShouldUseClassNameAsDefaultKeyspace() {
		assertThat(resolver.resolveKeySpace(TypeWithoutAnySettings.class), is(TypeWithoutAnySettings.class.getName()));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveShouldFavorConfiguredNameOverClassName() {

		config.addKeyspaceSettings(new KeyspaceSettings(TypeWithoutAnySettings.class, "ji'e'toh"));
		assertThat(resolver.resolveKeySpace(TypeWithoutAnySettings.class), is("ji'e'toh"));
	}

	static class TypeWithoutAnySettings {

	}

}
