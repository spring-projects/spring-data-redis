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
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.mapping.RedisMappingContext.ConfigAwareTimeToLiveAccessor;

/**
 * @author Christoph Strobl
 */
public class ConfigAwareTimeToLiveAccessorUnitTests {

	ConfigAwareTimeToLiveAccessor accessor;
	KeyspaceConfiguration config;

	@Before
	public void setUp() {

		config = new KeyspaceConfiguration();
		accessor = new ConfigAwareTimeToLiveAccessor(config, new RedisMappingContext());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test(expected = IllegalArgumentException.class)
	public void getTimeToLiveShouldThrowExceptionWhenSourceObjectIsNull() {
		accessor.getTimeToLive(null);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnNullIfNothingConfiguredOrAnnotated() {
		assertThat(accessor.getTimeToLive(new SimpleType()), nullValue());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnConfiguredValueForSimpleType() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleType.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleType()), is(10L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnValueWhenTypeIsAnnotated() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotation()), is(5L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveConsidersAnnotationOverConfig() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithRedisHashAnnotation.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotation()), is(5L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnValueWhenPropertyIsAnnotatedAndHasValue() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotationAndTTLProperty(20L)), is(20L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnValueFromTypeAnnotationWhenPropertyIsAnnotatedAndHasNullValue() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotationAndTTLProperty()), is(10L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnNullWhenPropertyIsAnnotatedAndHasNullValue() {
		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty()), nullValue());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldReturnConfiguredValueWhenPropertyIsAnnotatedAndHasNullValue() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleTypeWithTTLProperty.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty()), is(10L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getTimeToLiveShouldFavorAnnotatedNotNullPropertyValueOverConfiguredOne() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleTypeWithTTLProperty.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty(25L)), is(25L));
	}

	static class SimpleType {}

	static class SimpleTypeWithTTLProperty {

		@TimeToLive Long ttl;

		SimpleTypeWithTTLProperty() {}

		SimpleTypeWithTTLProperty(Long ttl) {
			this.ttl = ttl;
		}
	}

	@RedisHash(timeToLive = 5)
	static class TypeWithRedisHashAnnotation {}

	@RedisHash(timeToLive = 10)
	static class TypeWithRedisHashAnnotationAndTTLProperty {

		@TimeToLive Long ttl;

		TypeWithRedisHashAnnotationAndTTLProperty() {}

		TypeWithRedisHashAnnotationAndTTLProperty(Long ttl) {
			this.ttl = ttl;
		}
	}
}
