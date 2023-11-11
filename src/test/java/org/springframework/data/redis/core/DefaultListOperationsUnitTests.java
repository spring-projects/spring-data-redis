/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.redis.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Koy Zhuang
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultListOperationsUnitTests<K, V> {
    @Mock RedisConnectionFactory connectionFactoryMock;
    @Mock RedisConnection connectionMock;

    @Test
    void shouldCallPushToAllCollectionGivenRedisTemplateObject() {
        when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);

        final StringRedisSerializer keySerializer = mock(StringRedisSerializer.class);
        final JdkSerializationRedisSerializer valueSerializer = mock(JdkSerializationRedisSerializer.class);
        doReturn("any".getBytes()).when(keySerializer).serialize(anyString());
        doReturn("any".getBytes()).when(valueSerializer).serialize(any());

        // setup RedisTemplate<String, Object>
        RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
        template.setConnectionFactory(connectionFactoryMock);
        template.setKeySerializer(keySerializer);
        template.setValueSerializer(valueSerializer);
        template.afterPropertiesSet();

        final ListOperations<String, Object> listOperations = template.opsForList();
        final ListOperations<String, Object> spyTarget = spy(listOperations);

        List<V> values = List.of((V) (new Object()));
        spyTarget.leftPushAll("key", values);
        // should call leftPushAll(K key, V... values)
        verify(spyTarget, times(1)).leftPushAll(anyString(), any(Object.class));
        // forward to leftPushAll(K key, Collection<V> values)
        verify(spyTarget, times(1)).leftPushAll(anyString(), anyCollection());
    }
}
