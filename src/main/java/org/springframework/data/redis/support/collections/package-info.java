/**
 * Package providing implementations for most of the {@code java.util} collections on top of Redis.
 * <p/>
 * For indexed collections, such as {@link java.util.List}, {@link java.util.Queue} or {@link java.util.Deque} consider
 * {@link org.springframework.data.redis.support.collections.RedisList}.
 * <p/>
 * For collections without duplicates the obvious candidate is
 * {@link org.springframework.data.redis.support.collections.RedisSet}. Use
 * {@link org.springframework.data.redis.support.collections.RedisZSet} if a certain order is required.
 * </p/>
 * Lastly, for key/value associations {@link org.springframework.data.redis.support.collections.RedisMap} providing a
 * Map-like abstraction on top of a Redis hash.
 */
@org.springframework.lang.NonNullApi
@org.springframework.lang.NonNullFields
package org.springframework.data.redis.support.collections;
