package dev.kafka.dispatch.handler;

import dev.kafka.dispatch.exception.NotRetryableException;
import dev.kafka.dispatch.exception.RetryableException;
import dev.kafka.dispatch.service.DispatchService;
import dev.kafka.dispatch.message.OrderCreated;
import dev.kafka.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

        Exception exception = assertThrows(NotRetryableException.class, () -> handler.listen(0, key, testEvent));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: Service failure"));

        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }

    @Test
    public void testListen_ServiceThrowsRetryableException() throws Exception {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RetryableException("Service failure")).when(dispatchServiceMock).process(key, testEvent);

        Exception exception = assertThrows(RuntimeException.class, () -> handler.listen(0, key, testEvent));
        assertThat(exception.getMessage(), equalTo("Service failure"));
        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }
}