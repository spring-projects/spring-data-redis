/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.PartialUpdate;
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

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void getTimeToLiveShouldThrowExceptionWhenSourceObjectIsNull() {
		accessor.getTimeToLive(null);
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnNullIfNothingConfiguredOrAnnotated() {
		assertThat(accessor.getTimeToLive(new SimpleType()), nullValue());
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnConfiguredValueForSimpleType() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleType.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleType()), is(10L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnValueWhenTypeIsAnnotated() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotation()), is(5L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveConsidersAnnotationOverConfig() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithRedisHashAnnotation.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotation()), is(5L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnValueWhenPropertyIsAnnotatedAndHasValue() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotationAndTTLProperty(20L)), is(20L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnValueFromTypeAnnotationWhenPropertyIsAnnotatedAndHasNullValue() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotationAndTTLProperty()), is(10L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnNullWhenPropertyIsAnnotatedAndHasNullValue() {
		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty()), nullValue());
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnConfiguredValueWhenPropertyIsAnnotatedAndHasNullValue() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleTypeWithTTLProperty.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty()), is(10L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldFavorAnnotatedNotNullPropertyValueOverConfiguredOne() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleTypeWithTTLProperty.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty(25L)), is(25L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnMethodLevelTimeToLiveIfPresent() {
		assertThat(accessor.getTimeToLive(new TypeWithTtlOnMethod(10L)), is(10L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnConfiguredValueWhenMethodLevelTimeToLiveIfPresentButHasNullValue() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithTtlOnMethod.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithTtlOnMethod(null)), is(10L));
	}

	@Test // DATAREDIS-425
	public void getTimeToLiveShouldReturnValueWhenMethodLevelTimeToLiveIfPresentAlthoughConfiguredValuePresent() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithTtlOnMethod.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithTtlOnMethod(100L)), is(100L));
	}

	@Test // DATAREDIS-471
	public void getTimeToLiveShouldReturnDefaultValue() {

		Long ttl = accessor
				.getTimeToLive(new PartialUpdate<>("123", new TypeWithRedisHashAnnotation()));

		assertThat(ttl, is(5L));
	}

	@Test // DATAREDIS-471
	public void getTimeToLiveShouldReturnValueWhenUpdateModifiesTtlProperty() {

		Long ttl = accessor
				.getTimeToLive(new PartialUpdate<>("123", new SimpleTypeWithTTLProperty())
						.set("ttl", 100).refreshTtl(true));

		assertThat(ttl, is(100L));
	}

	@Test // DATAREDIS-471
	public void getTimeToLiveShouldReturnPropertyValueWhenUpdateModifiesTtlProperty() {

		Long ttl = accessor.getTimeToLive(
				new PartialUpdate<>("123",
				new TypeWithRedisHashAnnotationAndTTLProperty()).set("ttl", 100).refreshTtl(true));

		assertThat(ttl, is(100L));
	}

	@Test // DATAREDIS-471
	public void getTimeToLiveShouldReturnDefaultValueWhenUpdateDoesNotModifyTtlProperty() {

		Long ttl = accessor
				.getTimeToLive(new PartialUpdate<>("123",
				new TypeWithRedisHashAnnotationAndTTLProperty()).refreshTtl(true));

		assertThat(ttl, is(10L));
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

	static class TypeWithTtlOnMethod {

		Long value;

		public TypeWithTtlOnMethod(Long value) {
			this.value = value;
		}

		@TimeToLive
		Long getTimeToLive() {
			return value;
		}
	}
}
