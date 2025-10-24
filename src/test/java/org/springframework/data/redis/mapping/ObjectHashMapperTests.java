/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.redis.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.hash.ObjectHashMapper;

/**
 * Unit tests for {@link ObjectHashMapper}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
class ObjectHashMapperTests extends AbstractHashMapperTests {

	@SuppressWarnings("rawtypes")
	protected ObjectHashMapper mapperFor(Class type) {
		return new ObjectHashMapper();
	}

	@Test // DATAREDIS-503
	void testSimpleType() {
		assertBackAndForwardMapping(100);
	}

	@Test // DATAREDIS-503
	void fromHashShouldCastToType() {

		ObjectHashMapper objectHashMapper = new ObjectHashMapper();
		Map<byte[], byte[]> hash = objectHashMapper.toHash(100);

		Integer result = objectHashMapper.fromHash(hash, Integer.class);

		assertThat(result).isEqualTo(100);
	}

	@Test // DATAREDIS-503
	void fromHashShouldFailIfTypeDoesNotMatch() {

		ObjectHashMapper objectHashMapper = new ObjectHashMapper();
		Map<byte[], byte[]> hash = objectHashMapper.toHash(100);

		assertThatExceptionOfType(ClassCastException.class).isThrownBy(() -> objectHashMapper.fromHash(hash, String.class));
	}

	@Test // DATAREDIS-1179
	void hashMapperAllowsReuseOfRedisConverter/*and thus the MappingContext holding eg. TypeAlias information*/() {

		WithTypeAlias source = new WithTypeAlias();
		source.value = "val";
		Map<byte[], byte[]> hash = new ObjectHashMapper().toHash(source);

		RedisMappingContext ctx = new RedisMappingContext();
		ctx.setInitialEntitySet(Collections.singleton(WithTypeAlias.class));
		ctx.afterPropertiesSet();

		MappingRedisConverter mappingRedisConverter = new MappingRedisConverter(ctx, null, null);
		mappingRedisConverter.afterPropertiesSet();

		ObjectHashMapper objectHashMapper = new ObjectHashMapper(mappingRedisConverter);
		assertThat(objectHashMapper.fromHash(hash)).isEqualTo(source);
	}

	@TypeAlias("_42_")
	static class WithTypeAlias {

		private String value;

		public String getValue() {
			return this.value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof WithTypeAlias that)) {
				return false;
			}

			return Objects.equals(this.getValue(), that.getValue());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getValue());
		}
	}
}
