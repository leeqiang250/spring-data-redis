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

import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.serializer.SerializerFunction;
import org.springframework.util.Assert;

/**
 * Interface supporting key/value serialization using {@link SerializerFunction} and {@link Argument}.
 * 
 * @author Mark Paluch
 */
public interface ReactiveOperationsSupport<K, V> {

	SerializerFunction<K> toKey();

	SerializerFunction<V> toValue();

	default Mono<ByteBuffer> key(Argument<K> argument) {
		return argument.serialized(toKey());
	}

	default Flux<ByteBuffer> key(Arguments<K> argument) {
		return argument.serialized(toKey());
	}

	default Mono<ByteBuffer> value(Argument<V> argument) {
		return argument.serialized(toValue());
	}

	default Flux<ByteBuffer> value(Arguments<V> argument) {
		return argument.serialized(toValue());
	}

	static class Argument<K> {

		private final Mono<K> key;

		private Argument(Mono<K> key) {
			this.key = key;
		}

		static <K> Argument<K> just(K key) {
			Assert.notNull(key, "Key must not be null!");
			return new Argument<>(Mono.just(key));
		}

		Mono<ByteBuffer> serialized(SerializerFunction<K> serializerFunction) {
			return key.flatMap(serializerFunction::serialize).next();
		}
	}

	static class Arguments<K> {

		private final Flux<K> keys;

		private Arguments(Flux<K> keys) {
			this.keys = keys;
		}

		static <K> Arguments<K> fromArray(K[] keys) {

			Assert.notNull(keys, "Key must not be null!");

			return new Arguments<K>(Flux.fromArray(keys));
		}

		static <K> Arguments<K> from(Publisher<? extends K> keys) {

			Assert.notNull(keys, "Key must not be null!");

			return new Arguments<K>(Flux.from(keys));
		}

		Flux<ByteBuffer> serialized(SerializerFunction<K> serializerFunction) {
			return keys.flatMap(serializerFunction::serialize);
		}
	}
}
