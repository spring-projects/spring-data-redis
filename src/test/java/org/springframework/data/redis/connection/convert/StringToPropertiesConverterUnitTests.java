/*
 * Copyright 2025 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisSystemException;

/**
 * Unit tests for {@link StringToPropertiesConverter}.
 */
class StringToPropertiesConverterUnitTests {

	private final StringToPropertiesConverter converter = new StringToPropertiesConverter();

	@Test
	void convertShouldParsePropertiesCorrectly() {
		String source = "key1=value1\nkey2=value2";
		Properties properties = converter.convert(source);

		assertThat(properties).containsEntry("key1", "value1");
		assertThat(properties).containsEntry("key2", "value2");
	}

	@Test
	void convertShouldHandleEmptyString() {
		Properties properties = converter.convert("");
		assertThat(properties).isEmpty();
	}
    
    @Test
    void convertShouldThrowExceptionForNull() {
        // Converters.toProperties wraps the NPE in RedisSystemException
        assertThatExceptionOfType(RedisSystemException.class)
            .isThrownBy(() -> converter.convert(null));
    }
}
