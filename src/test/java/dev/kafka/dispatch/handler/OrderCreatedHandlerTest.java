package dev.kafka.dispatch.handler;

import dev.kafka.dispatch.service.DispatchService;
import dev.kafka.dispatch.message.OrderCreated;
import dev.kafka.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.*;

class OrderCreatedHandlerTest {

    private OrderCreatedHandler handler;
    private DispatchService dispatchServiceMock;

    @BeforeEach
    void setUp() {
        dispatchServiceMock = mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchServiceMock);
    }

    @Test
    void listen_success() throws Exception{
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        handler.listen(0, key, testEvent);
        verify(dispatchServiceMock, times(1)).process(key,testEvent);
    }

    @Test
    void listen_ServiceThrowsException() throws Exception{
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Service failure")).when(dispatchServiceMock).process(key, testEvent);

        handler.listen(0, key, testEvent);

        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }
}