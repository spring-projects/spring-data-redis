/*
 * Copyright 2013-present the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.util.Properties;

import org.springframework.core.convert.converter.Converter;

/**
 * Converts Strings in Redis {@code INFO} / {@code CLUSTER INFO} key:value format to {@link Properties}.
 * <p>
 * Unlike {@link Properties#load}, this converter does not interpret escape sequences (e.g. {@code \u}) so that values
 * containing backslashes — such as Windows-style file paths emitted by Redis on Windows — are preserved verbatim.
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 */
public class StringToPropertiesConverter implements Converter<String, Properties> {

	@Override
	public Properties convert(String source) {
		return Converters.toProperties(source);
	}
}
