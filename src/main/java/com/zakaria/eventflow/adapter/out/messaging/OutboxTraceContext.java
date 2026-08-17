package com.zakaria.eventflow.adapter.out.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class OutboxTraceContext {

    private static final W3CTraceContextPropagator PROPAGATOR = W3CTraceContextPropagator.getInstance();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {
    };
    private static final TextMapSetter<Map<String, String>> SETTER = Map::put;
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(@Nullable Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private final ObjectMapper objectMapper;

    public OutboxTraceContext(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Nullable
    public String capture() {
        Map<String, String> carrier = new HashMap<>();
        PROPAGATOR.inject(Context.current(), carrier, SETTER);
        if (carrier.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(carrier);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not serialize outbox trace context", failure);
        }
    }

    public void runWith(@Nullable String serializedCarrier, Runnable action) {
        Context restored = Context.root();
        if (serializedCarrier != null && !serializedCarrier.isBlank()) {
            restored = PROPAGATOR.extract(Context.root(), deserialize(serializedCarrier), GETTER);
        }
        try (Scope ignored = restored.makeCurrent()) {
            action.run();
        }
    }

    private Map<String, String> deserialize(String serializedCarrier) {
        try {
            return objectMapper.readValue(serializedCarrier, MAP_TYPE);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not deserialize outbox trace context", failure);
        }
    }
}
