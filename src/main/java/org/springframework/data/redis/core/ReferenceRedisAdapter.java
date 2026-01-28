package org.springframework.data.redis.core;

import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.convert.ReferenceMappingRedisConverter;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.lang.Nullable;

/**
 * Wrapper for {@link RedisKeyValueAdapter} with correct cyclic reference resolving
 *
 * @author Ilya Viaznin
 * @see RedisKeyValueAdapter
 */
public class ReferenceRedisAdapter extends RedisKeyValueAdapter {

    /**
     * Evaluation caching
     */
    private boolean isReferenceConverter;

    /**
     * Creates new {@link ReferenceRedisAdapter} with default {@link RedisMappingContext} and default
     * {@link RedisCustomConversions}.
     *
     * @param redisOps must not be {@literal null}.
     */
    public ReferenceRedisAdapter(RedisOperations<?, ?> redisOps) {
        this(redisOps, new RedisMappingContext());
    }

    /**
     * Creates new {@link ReferenceRedisAdapter} with default {@link RedisCustomConversions}.
     *
     * @param redisOps must not be {@literal null}.
     * @param mappingContext must not be {@literal null}.
     */
    public ReferenceRedisAdapter(RedisOperations<?, ?> redisOps, RedisMappingContext mappingContext) {
        this(redisOps, mappingContext, new RedisCustomConversions());
    }

    /**
     * Creates new {@link ReferenceRedisAdapter}.
     *
     * @param redisOps must not be {@literal null}.
     * @param mappingContext must not be {@literal null}.
     * @param customConversions can be {@literal null}.
     * @since 2.0
     */
    public ReferenceRedisAdapter(RedisOperations<?, ?> redisOps, RedisMappingContext mappingContext,
                                @Nullable org.springframework.data.convert.CustomConversions customConversions) {
        super(redisOps, mappingContext, customConversions);
    }

    /**
     * Creates new {@link ReferenceRedisAdapter} with specific {@link RedisConverter}.
     *
     * @param redisOps must not be {@literal null}.
     * @param redisConverter must not be {@literal null}.
     */
    public ReferenceRedisAdapter(RedisOperations<?, ?> redisOps, RedisConverter redisConverter) {
        super(redisOps, redisConverter);
        isReferenceConverter = redisConverter instanceof ReferenceMappingRedisConverter;
    }

    @Override
    public <T> T get(Object id, String keyspace, Class<T> type) {
        T val;
        if (isReferenceConverter) {
            var converter = (ReferenceMappingRedisConverter) getConverter();
            converter.clearResolvedCtx();
            val = super.get(id, keyspace, type);
            converter.clearResolvedCtx();
        }
        else
            val = super.get(id, keyspace, type);

        return val;
    }
}
