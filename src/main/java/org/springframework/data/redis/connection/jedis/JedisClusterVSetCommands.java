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
package org.springframework.data.redis.connection.jedis;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisVectorSetCommands;
import org.springframework.util.Assert;

import redis.clients.jedis.params.VAddParams;

/**
 * Cluster {@link RedisVectorSetCommands} implementation for Jedis.
 *
 * @author Anne Lee
 * @since 3.5
 */
@NullUnmarked
class JedisClusterVSetCommands implements RedisVectorSetCommands {


    private final JedisClusterConnection connection;

    JedisClusterVSetCommands(@NonNull JedisClusterConnection connection) {
        this.connection = connection;
    }

    @Override
    public Boolean vAdd(byte @NonNull [] key, byte @NonNull [] values, byte @NonNull [] element,
                        VAddOptions options) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.notNull(element, "Element must not be null");

        try {
            if (options == null) {
                return connection.getCluster().vaddFP32(key, values, element);
            }

            VAddParams params = JedisConverters.toVAddParams(options);

            if (options.getReduceDim() != null) {
                // With REDUCE dimension
                return connection.getCluster().vaddFP32(key, values, element, options.getReduceDim(), params);
            }

            return connection.getCluster().vaddFP32(key, values, element, params);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    @Override
    public Boolean vAdd(byte @NonNull [] key, double @NonNull [] values, byte @NonNull [] element,
                        VAddOptions options) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.notNull(element, "Element must not be null");

        // Convert double[] to float[] since Jedis uses float[]
        float[] floatValues = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            floatValues[i] = (float) values[i];
        }

        try {
            if (options == null) {
                return connection.getCluster().vadd(key, floatValues, element);
            }

            redis.clients.jedis.params.VAddParams params = JedisConverters.toVAddParams(options);

            if (options.getReduceDim() != null) {
                // With REDUCE dimension
                return connection.getCluster().vadd(key, floatValues, element, options.getReduceDim(), params);
            }

            return connection.getCluster().vadd(key, floatValues, element, params);
        } catch (Exception ex) {
            throw convertJedisAccessException(ex);
        }
    }

    private DataAccessException convertJedisAccessException(Exception ex) {
        return connection.convertJedisAccessException(ex);
    }
}