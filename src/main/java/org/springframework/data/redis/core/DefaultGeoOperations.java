/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.redis.core;

import org.springframework.data.redis.connection.RedisConnection;

/**
 * Default implementation of {@link GeoOperations}.
 *
 * @author Ninad Divadkar
 */
public class DefaultGeoOperations<K, M> extends AbstractOperations<K, M> implements GeoOperations<K, M> {
    DefaultGeoOperations(RedisTemplate<K, M> template) {
        super(template);
    }

    @Override
    public Long geoAdd(K key, final double longitude, final double latitude, M member) {
        final byte[] rawKey = rawKey(key);
        final byte[] rawMember = rawValue(member);

        return execute(new RedisCallback<Long>() {

            public Long doInRedis(RedisConnection connection) {
                return connection.geoAdd(rawKey, longitude, latitude, rawMember);
            }
        }, true);
    }
}
