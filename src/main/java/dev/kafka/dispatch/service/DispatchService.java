package dev.kafka.dispatch.service;

import dev.kafka.dispatch.message.DispatchPrepared;
import dev.kafka.dispatch.message.OrderCreated;
import dev.kafka.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static java.util.UUID.randomUUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class DispatchService {

    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final KafkaTemplate<String,Object> kafkaProducer;
    private static final UUID APPLICATION_ID = randomUUID();
    public void process(OrderCreated orderCreated) throws Exception{

        DispatchPrepared dispatchPrepared = DispatchPrepared.builder()
                .orderId(orderCreated.getOrderId())
                .build();
        kafkaProducer.send(DISPATCH_TRACKING_TOPIC, dispatchPrepared).get();

        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderCreated.getOrderId())
                .processedBy(APPLICATION_ID)
                .notes("Dispatched: "+ orderCreated.getItem())
                .build();
        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, orderDispatched).get();

        log.info("Sent Message: orderID: " + orderCreated.getOrderId() + " - processedByID: "+APPLICATION_ID);
    }
}
