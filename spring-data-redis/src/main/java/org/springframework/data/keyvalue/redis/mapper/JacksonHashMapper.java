/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.mapper;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * Mapper based on Jackson library.
 * 
 * @author Costin Leau
 */
public class JacksonHashMapper<T> implements HashMapper<T> {

	private final Class<T> type;
	private final ObjectMapper mapper;

	public JacksonHashMapper(Class<T> type) {
		this(type, new ObjectMapper());
	}

	public JacksonHashMapper(Class<T> type, ObjectMapper mapper) {
		this.type = type;
		this.mapper = mapper;
	}

	@Override
	public T fromHash(Map<?, ?> hash) {
		return mapper.convertValue(hash, type);
	}

	@Override
	public Map<?, ?> toHash(T object) {
		return mapper.convertValue(object, Map.class);
	}
}
