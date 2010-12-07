/**
 * Package providing implementations for most of the {@code java.util} collections on top of Redis.
 * <p/>
 * For indexed collections, such as {@link java.util.List}, {@link java.util.Queue} or {@link java.util.Deque} 
 * consider {@link RedisList}.<p/>
 * For collections without duplicates the obvious candidate is {@link RedisSet}. Use {@link RedisZSet} if a 
 * certain order is required.</p/>
 * Lastly, for key/value associations {@link RedisMap} providing a Map-like abstraction on top of a Redis hash.
 */
package org.springframework.data.keyvalue.redis.support.collections;

