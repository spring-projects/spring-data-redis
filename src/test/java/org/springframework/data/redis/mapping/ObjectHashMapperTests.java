/*
 * Copyright 2016-2020 the original author or authors.
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

import lombok.Data;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.hash.ObjectHashMapper;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ObjectHashMapperTests extends AbstractHashMapperTest {

	protected ObjectHashMapper mapperFor(Class t) {
		return new ObjectHashMapper();
	}

	@Test // DATAREDIS-503
	public void testSimpleType() {
		assertBackAndForwardMapping(new Integer(100));
	}

	@Test // DATAREDIS-503
	public void fromHashShouldCastToType() {

		ObjectHashMapper objectHashMapper = new ObjectHashMapper();
		Map<byte[], byte[]> hash = objectHashMapper.toHash(new Integer(100));

		Integer result = objectHashMapper.fromHash(hash, Integer.class);

		assertThat(result).isEqualTo(new Integer(100));
	}

	@Test(expected = ClassCastException.class) // DATAREDIS-503
	public void fromHashShouldFailIfTypeDoesNotMatch() {

		ObjectHashMapper objectHashMapper = new ObjectHashMapper();
		Map<byte[], byte[]> hash = objectHashMapper.toHash(new Integer(100));

		String result = objectHashMapper.fromHash(hash, String.class);
	}

	@Test // DATAREDIS-1179
	public void hashMapperAllowsReuseOfRedisConverter/*and thus the MappingContext holding eg. TypeAlias information*/() {

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
	@Data
	static class WithTypeAlias {
		String value;
	}

}
