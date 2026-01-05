/*
 * Copyright 2022-present the original author or authors.
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
import java.io.InputStream;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Defines the contract for Object Mapping readers. Implementations of this interface can deserialize a given byte array
 * holding JSON to an Object considering the target type.
 * <p>
 * Reader functions can customize how the actual JSON is being deserialized by e.g. obtaining a customized
 * {@link com.fasterxml.jackson.databind.ObjectReader} applying serialization features, date formats, or views.
 *
 * @author Mark Paluch
 * @since 3.0
 */
@FunctionalInterface
public interface JacksonObjectReader {

	/**
	 * Read an object graph from the given root JSON into a Java object considering the {@link JavaType}.
	 *
	 * @param mapper the object mapper to use.
	 * @param source the JSON to deserialize.
	 * @param type the Java target type
	 * @return the deserialized Java object.
	 * @throws IOException if an I/O error or JSON deserialization error occurs.
	 */
	Object read(ObjectMapper mapper, byte[] source, JavaType type) throws IOException;

	/**
	 * Create a default {@link JacksonObjectReader} delegating to {@link ObjectMapper#readValue(InputStream, JavaType)}.
	 *
	 * @return the default {@link JacksonObjectReader}.
	 */
	static JacksonObjectReader create() {
		return (mapper, source, type) -> mapper.readValue(source, 0, source.length, type);
	}

}
