package com.loggingservice.agentbasedservice.services;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class RedisService {

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    public Mono<Boolean> save(String key,
                              Object value,
                              Duration ttl) {

        return redisTemplate
                .opsForValue()
                .set(key, value, ttl);
    }

    public Mono<Object> get(String key) {

        return redisTemplate
                .opsForValue()
                .get(key);
    }

    public Mono<Boolean> delete(String key) {

        return redisTemplate
                .delete(key)
                .map(count -> count > 0);
    }

    public Mono<Boolean> exists(String key) {

        return redisTemplate
                .hasKey(key);
    }
}