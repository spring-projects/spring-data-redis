/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.annotation;

import org.jspecify.annotations.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.util.InvalidMimeTypeException;
import org.springframework.util.MimeType;

/**
 * @author Ilyass Bougati
 */
public class RedisContentTypeResolver extends DefaultContentTypeResolver {
    @Override
    public @Nullable MimeType resolve(@Nullable MessageHeaders headers) throws InvalidMimeTypeException {
        assert headers != null;
        Object contentType = headers.get(MessageHeaders.CONTENT_TYPE);

        if (contentType instanceof MimeType mimeType) {
            return mimeType;
        }
        if (contentType instanceof String str) {
            return MimeType.valueOf(str);
        }

        // If nothing is specified, we return null to let the converters
        // try their default behavior.
        return null;
    }
}
