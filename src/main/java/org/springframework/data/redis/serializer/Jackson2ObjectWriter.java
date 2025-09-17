/*
 * Copyright 2022-2025 the original author or authors.
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
package org.springframework.data.redis.serializer;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Defines the contract for Object Mapping writers. Implementations of this interface can serialize a given Object to a
 * {@code byte[]} containing JSON.
 * <p>
 * Writer functions can customize how the actual JSON is being written by e.g. obtaining a customized
 * {@link com.fasterxml.jackson.databind.ObjectWriter} applying serialization features, date formats, or views.
 *
 * @author Mark Paluch
 * @since 3.0
 * @deprecated since 4.0 in favor of {@link JacksonObjectWriter}.
 */
@FunctionalInterface
@Deprecated(since = "4.0", forRemoval = true)
public interface Jackson2ObjectWriter {

	/**
	 * Write the object graph with the given root {@code source} as byte array.
	 *
	 * @param mapper the object mapper to use.
	 * @param source the root of the object graph to marshal.
	 * @return a byte array containing the serialized object graph.
	 * @throws IOException if an I/O error or JSON serialization error occurs.
	 */
	byte[] write(ObjectMapper mapper, Object source) throws IOException;

	/**
	 * Create a default {@link Jackson2ObjectWriter} delegating to {@link ObjectMapper#writeValueAsBytes(Object)}.
	 *
	 * @return the default {@link Jackson2ObjectWriter}.
	 */
	static Jackson2ObjectWriter create() {
		return ObjectMapper::writeValueAsBytes;
	}

}
