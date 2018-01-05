/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.connection.convert;

import java.io.StringReader;
import java.util.Properties;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.RedisSystemException;

/**
 * Converts Strings to {@link Properties}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 */
public class StringToPropertiesConverter implements Converter<String, Properties> {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(Object)
	 */
	@Override
	public Properties convert(String source) {

		Properties info = new Properties();
		try (StringReader stringReader = new StringReader(source)) {
			info.load(stringReader);
		} catch (Exception ex) {
			throw new RedisSystemException("Cannot read Redis info", ex);
		}
		return info;
	}
}
