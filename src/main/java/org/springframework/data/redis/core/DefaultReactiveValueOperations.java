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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.SerializerFunction;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
public class DefaultReactiveValueOperations<K, V>
		implements ReactiveValueOperations<K, V>, ReactiveOperationsSupport<K, V> {

	private ReactiveRedisTemplate<K, V> template;

	public DefaultReactiveValueOperations(ReactiveRedisTemplate<K, V> template) {
		this.template = template;
	}

	@Override
	public Mono<Boolean> set(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> Mono.when(key(Argument.just(key)), value(Argument.just(value)))
				.flatMap(t -> connection.stringCommands().set(t.getT1(), t.getT2())));
	}

	@Override
	public Mono<Boolean> set(K key, V value, long timeout, TimeUnit unit) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(unit, "TimeUnit must not be null!");

		return template.createMono(connection -> Mono.when(key(Argument.just(key)), value(Argument.just(value))).flatMap(
				t -> connection.stringCommands().set(t.getT1(), t.getT2(), Expiration.from(timeout, unit), SetOption.UPSERT)));
	}

	@Override
	public Mono<Boolean> setIfAbsent(K key, V value) {

		Assert.notNull(key, "Key must not be null!");

		return template.createMono(connection -> Mono.when(key(Argument.just(key)), value(Argument.just(value))).flatMap(
				t -> connection.stringCommands().set(t.getT1(), t.getT2(), Expiration.persistent(), SetOption.UPSERT)));
	}

	@Override
	public Mono<Void> multiSet(Map<? extends K, ? extends V> map) {
		return null;
	}

	@Override
	public Mono<Boolean> multiSetIfAbsent(Map<? extends K, ? extends V> map) {
		return null;
	}

	@Override
	public Mono<V> get(Object key) {
		return null;
	}

	@Override
	public Mono<V> getAndSet(K key, V value) {
		return null;
	}

	@Override
	public Flux<V> multiGet(Collection<K> keys) {
		return null;
	}

	@Override
	public Mono<Long> increment(K key, long delta) {
		return null;
	}

	@Override
	public Mono<Double> increment(K key, double delta) {
		return null;
	}

	@Override
	public Mono<Integer> append(K key, String value) {
		return null;
	}

	@Override
	public Mono<String> get(K key, long start, long end) {
		return null;
	}

	@Override
	public Mono<Void> set(K key, V value, long offset) {
		return null;
	}

	@Override
	public Mono<Long> size(K key) {
		return null;
	}

	@Override
	public Mono<Boolean> setBit(K key, long offset, boolean value) {
		return null;
	}

	@Override
	public Mono<Boolean> getBit(K key, long offset) {
		return null;
	}

	@Override
	public ReactiveRedisOperations<K, V> getOperations() {
		return template;
	}

	@Override
	public SerializerFunction<K> toKey() {
		return template.toKey();
	}

	@Override
	public SerializerFunction<V> toValue() {
		return template.toValue();
	}
}
