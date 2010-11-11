/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.connection.jedis;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPipeline;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.Client.LIST_POSITION;

/**
 * Wrapper class used for returning to the pool the Jedis connections,
 * once they are closed.
 * 
 * @author Costin Leau
 */
class JedisPoolWrapper extends Jedis {

	private final Jedis delegate;
	private final JedisPool pool;

	/**
	 * Constructs a new <code>JedisPoolWrapper</code> instance.
	 *
	 * @param host
	 * @param delegate
	 */
	public JedisPoolWrapper(Jedis delegate, JedisPool pool) {
		super((String) null);
		this.delegate = delegate;
		this.pool = pool;
	}

	public Integer append(String key, String value) {
		return delegate.append(key, value);
	}

	public String auth(String password) {
		return delegate.auth(password);
	}

	public String bgrewriteaof() {
		return delegate.bgrewriteaof();
	}

	public String bgsave() {
		return delegate.bgsave();
	}

	public List<String> blpop(int timeout, String... keys) {
		return delegate.blpop(timeout, keys);
	}

	public List<String> brpop(int timeout, String... keys) {
		return delegate.brpop(timeout, keys);
	}

	public List<String> configGet(String pattern) {
		return delegate.configGet(pattern);
	}

	public String configSet(String parameter, String value) {
		return delegate.configSet(parameter, value);
	}

	public void connect() throws UnknownHostException, IOException {
		delegate.connect();
	}

	public Integer dbSize() {
		return delegate.dbSize();
	}

	public String debug(DebugParams params) {
		return delegate.debug(params);
	}

	public Integer decr(String key) {
		return delegate.decr(key);
	}

	public Integer decrBy(String key, int integer) {
		return delegate.decrBy(key, integer);
	}

	public Integer del(String... keys) {
		return delegate.del(keys);
	}

	public void disconnect() throws IOException {
		pool.returnResource(delegate);
	}

	public String echo(String string) {
		return delegate.echo(string);
	}

	public boolean equals(Object obj) {
		return delegate.equals(obj);
	}

	public Integer exists(String key) {
		return delegate.exists(key);
	}

	public Integer expire(String key, int seconds) {
		return delegate.expire(key, seconds);
	}

	public Integer expireAt(String key, long unixTime) {
		return delegate.expireAt(key, unixTime);
	}

	public String flushAll() {
		return delegate.flushAll();
	}

	public String flushDB() {
		return delegate.flushDB();
	}

	public String get(String key) {
		return delegate.get(key);
	}

	public String getSet(String key, String value) {
		return delegate.getSet(key, value);
	}

	public int hashCode() {
		return delegate.hashCode();
	}

	public Integer hdel(String key, String field) {
		return delegate.hdel(key, field);
	}

	public Integer hexists(String key, String field) {
		return delegate.hexists(key, field);
	}

	public String hget(String key, String field) {
		return delegate.hget(key, field);
	}

	public Map<String, String> hgetAll(String key) {
		return delegate.hgetAll(key);
	}

	public Integer hincrBy(String key, String field, int value) {
		return delegate.hincrBy(key, field, value);
	}

	public List<String> hkeys(String key) {
		return delegate.hkeys(key);
	}

	public Integer hlen(String key) {
		return delegate.hlen(key);
	}

	public List<String> hmget(String key, String... fields) {
		return delegate.hmget(key, fields);
	}

	public String hmset(String key, Map<String, String> hash) {
		return delegate.hmset(key, hash);
	}

	public Integer hset(String key, String field, String value) {
		return delegate.hset(key, field, value);
	}

	public Integer hsetnx(String key, String field, String value) {
		return delegate.hsetnx(key, field, value);
	}

	public List<String> hvals(String key) {
		return delegate.hvals(key);
	}

	public Integer incr(String key) {
		return delegate.incr(key);
	}

	public Integer incrBy(String key, int integer) {
		return delegate.incrBy(key, integer);
	}

	public String info() {
		return delegate.info();
	}

	public boolean isConnected() {
		return delegate.isConnected();
	}

	public List<String> keys(String pattern) {
		return delegate.keys(pattern);
	}

	public Integer lastsave() {
		return delegate.lastsave();
	}

	public String lindex(String key, int index) {
		return delegate.lindex(key, index);
	}

	public Integer linsert(String key, LIST_POSITION where, String pivot, String value) {
		return delegate.linsert(key, where, pivot, value);
	}

	public Integer llen(String key) {
		return delegate.llen(key);
	}

	public String lpop(String key) {
		return delegate.lpop(key);
	}

	public Integer lpush(String key, String string) {
		return delegate.lpush(key, string);
	}

	public Integer lpushx(String key, String string) {
		return delegate.lpushx(key, string);
	}

	public List<String> lrange(String key, int start, int end) {
		return delegate.lrange(key, start, end);
	}

	public Integer lrem(String key, int count, String value) {
		return delegate.lrem(key, count, value);
	}

	public String lset(String key, int index, String value) {
		return delegate.lset(key, index, value);
	}

	public String ltrim(String key, int start, int end) {
		return delegate.ltrim(key, start, end);
	}

	public List<String> mget(String... keys) {
		return delegate.mget(keys);
	}

	public void monitor(JedisMonitor jedisMonitor) {
		delegate.monitor(jedisMonitor);
	}

	public Integer move(String key, int dbIndex) {
		return delegate.move(key, dbIndex);
	}

	public String mset(String... keysvalues) {
		return delegate.mset(keysvalues);
	}

	public Integer msetnx(String... keysvalues) {
		return delegate.msetnx(keysvalues);
	}

	public Transaction multi() {
		return delegate.multi();
	}

	public List<Object> multi(TransactionBlock jedisTransaction) {
		return delegate.multi(jedisTransaction);
	}

	public Integer persist(String key) {
		return delegate.persist(key);
	}

	public String ping() {
		return delegate.ping();
	}

	public List<Object> pipelined(JedisPipeline jedisPipeline) {
		return delegate.pipelined(jedisPipeline);
	}

	public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
		delegate.psubscribe(jedisPubSub, patterns);
	}

	public Integer publish(String channel, String message) {
		return delegate.publish(channel, message);
	}

	public void quit() {
		pool.returnResource(delegate);
	}

	public String randomKey() {
		return delegate.randomKey();
	}

	public String rename(String oldkey, String newkey) {
		return delegate.rename(oldkey, newkey);
	}

	public Integer renamenx(String oldkey, String newkey) {
		return delegate.renamenx(oldkey, newkey);
	}

	public String rpop(String key) {
		return delegate.rpop(key);
	}

	public String rpoplpush(String srckey, String dstkey) {
		return delegate.rpoplpush(srckey, dstkey);
	}

	public Integer rpush(String key, String string) {
		return delegate.rpush(key, string);
	}

	public Integer rpushx(String key, String string) {
		return delegate.rpushx(key, string);
	}

	public Integer sadd(String key, String member) {
		return delegate.sadd(key, member);
	}

	public String save() {
		return delegate.save();
	}

	public Integer scard(String key) {
		return delegate.scard(key);
	}

	public Set<String> sdiff(String... keys) {
		return delegate.sdiff(keys);
	}

	public Integer sdiffstore(String dstkey, String... keys) {
		return delegate.sdiffstore(dstkey, keys);
	}

	public String select(int index) {
		return delegate.select(index);
	}

	public String set(String key, String value) {
		return delegate.set(key, value);
	}

	public String setex(String key, int seconds, String value) {
		return delegate.setex(key, seconds, value);
	}

	public Integer setnx(String key, String value) {
		return delegate.setnx(key, value);
	}

	public String shutdown() {
		return delegate.shutdown();
	}

	public Set<String> sinter(String... keys) {
		return delegate.sinter(keys);
	}

	public Integer sinterstore(String dstkey, String... keys) {
		return delegate.sinterstore(dstkey, keys);
	}

	public Integer sismember(String key, String member) {
		return delegate.sismember(key, member);
	}

	public String slaveof(String host, int port) {
		return delegate.slaveof(host, port);
	}

	public String slaveofNoOne() {
		return delegate.slaveofNoOne();
	}

	public Set<String> smembers(String key) {
		return delegate.smembers(key);
	}

	public Integer smove(String srckey, String dstkey, String member) {
		return delegate.smove(srckey, dstkey, member);
	}

	public Integer sort(String key, SortingParams sortingParameters, String dstkey) {
		return delegate.sort(key, sortingParameters, dstkey);
	}

	public List<String> sort(String key, SortingParams sortingParameters) {
		return delegate.sort(key, sortingParameters);
	}

	public Integer sort(String key, String dstkey) {
		return delegate.sort(key, dstkey);
	}

	public List<String> sort(String key) {
		return delegate.sort(key);
	}

	public String spop(String key) {
		return delegate.spop(key);
	}

	public String srandmember(String key) {
		return delegate.srandmember(key);
	}

	public Integer srem(String key, String member) {
		return delegate.srem(key, member);
	}

	public Integer strlen(String key) {
		return delegate.strlen(key);
	}

	public void subscribe(JedisPubSub jedisPubSub, String... channels) {
		delegate.subscribe(jedisPubSub, channels);
	}

	public String substr(String key, int start, int end) {
		return delegate.substr(key, start, end);
	}

	public Set<String> sunion(String... keys) {
		return delegate.sunion(keys);
	}

	public Integer sunionstore(String dstkey, String... keys) {
		return delegate.sunionstore(dstkey, keys);
	}

	public void sync() {
		delegate.sync();
	}

	public String toString() {
		return delegate.toString();
	}

	public Integer ttl(String key) {
		return delegate.ttl(key);
	}

	public String type(String key) {
		return delegate.type(key);
	}

	public String unwatch() {
		return delegate.unwatch();
	}

	public String watch(String key) {
		return delegate.watch(key);
	}

	public Integer zadd(String key, double score, String member) {
		return delegate.zadd(key, score, member);
	}

	public Integer zcard(String key) {
		return delegate.zcard(key);
	}

	public Integer zcount(String key, double min, double max) {
		return delegate.zcount(key, min, max);
	}

	public Double zincrby(String key, double score, String member) {
		return delegate.zincrby(key, score, member);
	}

	public Integer zinterstore(String dstkey, String... sets) {
		return delegate.zinterstore(dstkey, sets);
	}

	public Integer zinterstore(String dstkey, ZParams params, String... sets) {
		return delegate.zinterstore(dstkey, params, sets);
	}

	public Set<String> zrange(String key, int start, int end) {
		return delegate.zrange(key, start, end);
	}

	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		return delegate.zrangeByScore(key, min, max, offset, count);
	}

	public Set<String> zrangeByScore(String key, double min, double max) {
		return delegate.zrangeByScore(key, min, max);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		return delegate.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		return delegate.zrangeByScoreWithScores(key, min, max);
	}

	public Set<Tuple> zrangeWithScores(String key, int start, int end) {
		return delegate.zrangeWithScores(key, start, end);
	}

	public Integer zrank(String key, String member) {
		return delegate.zrank(key, member);
	}

	public Integer zrem(String key, String member) {
		return delegate.zrem(key, member);
	}

	public Integer zremrangeByRank(String key, int start, int end) {
		return delegate.zremrangeByRank(key, start, end);
	}

	public Integer zremrangeByScore(String key, double start, double end) {
		return delegate.zremrangeByScore(key, start, end);
	}

	public Set<String> zrevrange(String key, int start, int end) {
		return delegate.zrevrange(key, start, end);
	}

	public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		return delegate.zrevrangeWithScores(key, start, end);
	}

	public Integer zrevrank(String key, String member) {
		return delegate.zrevrank(key, member);
	}

	public Double zscore(String key, String member) {
		return delegate.zscore(key, member);
	}

	public Integer zunionstore(String dstkey, String... sets) {
		return delegate.zunionstore(dstkey, sets);
	}

	public Integer zunionstore(String dstkey, ZParams params, String... sets) {
		return delegate.zunionstore(dstkey, params, sets);
	}
}