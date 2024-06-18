package dev.kafka.dispatch.util;

import dev.kafka.dispatch.message.DispatchPrepared;
import dev.kafka.dispatch.message.OrderCreated;
import java.util.UUID;

public class TestEventData {

    public static OrderCreated buildOrderCreatedEvent(UUID orderId, String item){
    return OrderCreated.builder()
            .orderId(orderId)
            .item(item)
            .build();
    }

    public static DispatchPrepared dispatchPreparedEvent(UUID orderId){
        return DispatchPrepared.builder()
                .orderId(orderId)
                .build();
    }
}
