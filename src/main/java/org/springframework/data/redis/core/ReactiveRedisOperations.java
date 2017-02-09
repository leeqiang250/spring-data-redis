/*
 * Copyright 2017 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Interface that specified a basic set of Redis operations, implemented by {@link ReactiveRedisTemplate}. Not often
 * used but a useful option for extensibility and testability (as it can be easily mocked or stubbed).
 * 
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveRedisOperations<K, V> {

	/**
	 * Executes the given action within a Redis connection. Application exceptions thrown by the action object get
	 * propagated to the caller (can only be unchecked) whenever possible. Redis exceptions are transformed into
	 * appropriate DAO ones. Allows for returning a result object, that is a domain object or a collection of domain
	 * objects. Performs automatic serialization/deserialization for the given objects to and from binary data suitable
	 * for the Redis storage. Note: Callback code is not supposed to handle transactions itself! Use an appropriate
	 * transaction manager. Generally, callback code must not touch any Connection lifecycle methods, like close, to let
	 * the template do its work.
	 * 
	 * @param <T> return type
	 * @param action callback object that specifies the Redis action
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> Flux<T> execute(ReactiveRedisCallback<T> action);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Keys
	// -------------------------------------------------------------------------

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	Mono<Boolean> hasKey(K key);

	/**
	 * Delete given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Mono<Long> delete(K... key);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Mono<Long> delete(Publisher<K> keys);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	Mono<DataType> type(K key);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	Flux<K> keys(K pattern);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	Mono<K> randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	Mono<Boolean> rename(K oldKey, K newKey);

	/**
	 * Rename key {@code oleName} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	Mono<Boolean> renameIfAbsent(K oldKey, K newKey);

	/**
	 * Set time to live for given {@code key}..
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param unit must not be {@literal null}.
	 * @return
	 */
	// Mono<Boolean> expire(K key, long timeout, TimeUnit unit);

	/**
	 * Set the expiration for given {@code key} as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param date must not be {@literal null}.
	 * @return
	 */
	// Mono<Boolean> expireAt(K key, Date date);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	// Mono<Boolean> persist(K key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	// Mono<Boolean> move(K key, int dbIndex);

	/**
	 * Retrieve serialized version of the value stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/dump">Redis Documentation: DUMP</a>
	 */
	// Mono<ByteBuffer> dump(K key);

	/**
	 * Create {@code key} using the {@code serializedValue}, previously obtained using {@link #dump(Object)}.
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param timeToLive
	 * @param unit must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/restore">Redis Documentation: RESTORE</a>
	 */
	// Mono<Void> restore(K key, byte[] value, long timeToLive, TimeUnit unit);

	/**
	 * Get the time to live for {@code key} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/ttl">Redis Documentation: TTL</a>
	 */
	// Mono<Long> getExpire(K key);

	/**
	 * Get the time to live for {@code key} in and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @return
	 */
	// Mono<Long> getExpire(K key, TimeUnit timeUnit);

	/**
	 * Sort the elements for {@code query}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the results of sort.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	// Flux<V> sort(SortQuery<K> query);

	/**
	 * Sort the elements for {@code query} applying {@link RedisSerializer}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	// <T> Flux<T> sort(SortQuery<K> query, RedisSerializer<T> resultSerializer);

	/**
	 * Sort the elements for {@code query} applying {@link BulkMapper}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	// <T> Flux<T> sort(SortQuery<K> query, BulkMapper<T, V> bulkMapper);

	/**
	 * Sort the elements for {@code query} applying {@link BulkMapper} and {@link RedisSerializer}.
	 *
	 * @param query must not be {@literal null}.
	 * @return the deserialized results of sort.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	// <T, S> Flux<T> sort(SortQuery<K> query, BulkMapper<T, S> bulkMapper, RedisSerializer<S> resultSerializer);

	/**
	 * Sort the elements for {@code query} and store result in {@code storeKey}.
	 *
	 * @param query must not be {@literal null}.
	 * @param storeKey must not be {@literal null}.
	 * @return number of values.
	 * @see <a href="http://redis.io/commands/sort">Redis Documentation: SORT</a>
	 */
	// Mono<Long> sort(SortQuery<K> query, K storeKey);

	// -------------------------------------------------------------------------
	// Methods to obtain specific operations interface objects.
	// -------------------------------------------------------------------------

	// operation types
	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology).
	 * 
	 * @return value operations
	 */
	ReactiveValueOperations<K, V> opsForValue();

	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology) bound to the given key.
	 * 
	 * @param key Redis key
	 * @return value operations bound to the given key
	 */
	//BoundValueOperations<K, V> boundValueOps(K key);

}
