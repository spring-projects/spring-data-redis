/*
 * Copyright 2013-2025 the original author or authors.
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
import org.springframework.lang.Nullable;

/**
 * Converts Strings to {@link Properties}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @deprecated since 3.4 in favor of {@link Converters#toProperties(String)}.
 */
@Deprecated(since = "3.4", forRemoval = true)
public class StringToPropertiesConverter implements Converter<String, Properties> {

	@Override
	public Properties convert(@Nullable String source) {
		return Converters.toProperties(source);
	}
}
