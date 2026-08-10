package com.loggingservice.agentbasedservice.services;

import com.loggingservice.agentbasedservice.model.ClientSession;
import com.loggingservice.agentbasedservice.model.ClientValidationResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

@Service
@RequiredArgsConstructor
public class JwtService {
    private final RedisService redisService;

    public Mono<Boolean> isValid(String token) {
        return redisService.exists(token)
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.just(true);
                    }

                    return validate(token)
                            .flatMap(validation -> {

                                if (!validation.isValid()) {
                                    return Mono.just(false);
                                }

                                ClientSession session = new ClientSession(
                                        validation.getClientId(),
                                        validation.getHostName(),
                                        validation.getExpiryTime(),
                                        token
                                );

                                Duration ttl = Duration.ofHours(24);

                                Instant expiry = Instant.parse(validation.getExpiryTime());

                                if (expiry.isBefore(Instant.now().plus(ttl))) {
                                    ttl = Duration.between(Instant.now(), expiry);
                                }

                                return redisService.save(token, session, ttl)
                                        .thenReturn(true);
                            });
                });
    }

    private String generateHash(String token) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(token.getBytes(StandardCharsets.UTF_8))
            );
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private Mono<ClientValidationResponse> validate(String token) {

        ClientValidationResponse response = new ClientValidationResponse(
                true,
                "client-" + System.currentTimeMillis(),
                "host-" + (int) (Math.random() * 100),
                Instant.now().plus(Duration.ofHours(2)).toString()
        );

        return Mono.just(response);
    }
}
