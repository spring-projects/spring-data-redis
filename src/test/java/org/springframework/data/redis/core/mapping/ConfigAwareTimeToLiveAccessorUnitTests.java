/*
 * Copyright 2015-2021 the original author or authors.
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
package org.springframework.data.redis.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.mapping.RedisMappingContext.ConfigAwareTimeToLiveAccessor;

/**
 * Unit tests for {@link ConfigAwareTimeToLiveAccessor}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class ConfigAwareTimeToLiveAccessorUnitTests {

	private ConfigAwareTimeToLiveAccessor accessor;
	private KeyspaceConfiguration config;

	@BeforeEach
	void setUp() {

		config = new KeyspaceConfiguration();
		accessor = new ConfigAwareTimeToLiveAccessor(config, new RedisMappingContext());
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldThrowExceptionWhenSourceObjectIsNull() {
		assertThatIllegalArgumentException().isThrownBy(() -> accessor.getTimeToLive(null));
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnNullIfNothingConfiguredOrAnnotated() {
		assertThat(accessor.getTimeToLive(new SimpleType())).isNull();
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnConfiguredValueForSimpleType() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleType.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleType())).isEqualTo(10L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnValueWhenTypeIsAnnotated() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotation())).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveConsidersAnnotationOverConfig() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithRedisHashAnnotation.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotation())).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnValueWhenPropertyIsAnnotatedAndHasValue() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotationAndTTLProperty(20L))).isEqualTo(20L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnValueFromTypeAnnotationWhenPropertyIsAnnotatedAndHasNullValue() {
		assertThat(accessor.getTimeToLive(new TypeWithRedisHashAnnotationAndTTLProperty())).isEqualTo(10L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnNullWhenPropertyIsAnnotatedAndHasNullValue() {
		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty())).isNull();
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnConfiguredValueWhenPropertyIsAnnotatedAndHasNullValue() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleTypeWithTTLProperty.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty())).isEqualTo(10L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldFavorAnnotatedNotNullPropertyValueOverConfiguredOne() {

		KeyspaceSettings setting = new KeyspaceSettings(SimpleTypeWithTTLProperty.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new SimpleTypeWithTTLProperty(25L))).isEqualTo(25L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnMethodLevelTimeToLiveIfPresent() {
		assertThat(accessor.getTimeToLive(new TypeWithTtlOnMethod(10L))).isEqualTo(10L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnConfiguredValueWhenMethodLevelTimeToLiveIfPresentButHasNullValue() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithTtlOnMethod.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithTtlOnMethod(null))).isEqualTo(10L);
	}

	@Test // DATAREDIS-425
	void getTimeToLiveShouldReturnValueWhenMethodLevelTimeToLiveIfPresentAlthoughConfiguredValuePresent() {

		KeyspaceSettings setting = new KeyspaceSettings(TypeWithTtlOnMethod.class, null);
		setting.setTimeToLive(10L);
		config.addKeyspaceSettings(setting);

		assertThat(accessor.getTimeToLive(new TypeWithTtlOnMethod(100L))).isEqualTo(100L);
	}

	@Test // DATAREDIS-538
	void getTimeToLiveShouldReturnMethodLevelTimeToLiveOfNonPublicTypeIfPresent() {
		assertThat(accessor.getTimeToLive(new PrivateTypeWithTtlOnMethod(10L))).isEqualTo(10L);
	}

	@Test // DATAREDIS-471
	void getTimeToLiveShouldReturnDefaultValue() {

		Long ttl = accessor
				.getTimeToLive(new PartialUpdate<>("123", new TypeWithRedisHashAnnotation()));

		assertThat(ttl).isEqualTo(5L);
	}

	@Test // DATAREDIS-471
	void getTimeToLiveShouldReturnValueWhenUpdateModifiesTtlProperty() {

		Long ttl = accessor
				.getTimeToLive(new PartialUpdate<>("123", new SimpleTypeWithTTLProperty())
						.set("ttl", 100).refreshTtl(true));

		assertThat(ttl).isEqualTo(100L);
	}

	@Test // DATAREDIS-471
	void getTimeToLiveShouldReturnPropertyValueWhenUpdateModifiesTtlProperty() {

		Long ttl = accessor.getTimeToLive(
				new PartialUpdate<>("123",
				new TypeWithRedisHashAnnotationAndTTLProperty()).set("ttl", 100).refreshTtl(true));

		assertThat(ttl).isEqualTo(100L);
	}

	@Test // DATAREDIS-471
	void getTimeToLiveShouldReturnDefaultValueWhenUpdateDoesNotModifyTtlProperty() {

		Long ttl = accessor
				.getTimeToLive(new PartialUpdate<>("123",
				new TypeWithRedisHashAnnotationAndTTLProperty()).refreshTtl(true));

		assertThat(ttl).isEqualTo(10L);
	}

	private static class SimpleType {}

	static class SimpleTypeWithTTLProperty {

		@TimeToLive Long ttl;

		SimpleTypeWithTTLProperty() {}

		SimpleTypeWithTTLProperty(Long ttl) {
			this.ttl = ttl;
		}
	}

	@RedisHash(timeToLive = 5)
	private static class TypeWithRedisHashAnnotation {}

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

		TypeWithTtlOnMethod(Long value) {
			this.value = value;
		}

		@TimeToLive
		private Long getTimeToLive() {
			return value;
		}
	}

	// Type must be private so it does not fall in the
	// package-default scope like the types from above
	private static class PrivateTypeWithTtlOnMethod {

		Long value;

		PrivateTypeWithTtlOnMethod(Long value) {
			this.value = value;
		}

		@TimeToLive
		Long getTimeToLive() {
			return value;
		}
	}
}
