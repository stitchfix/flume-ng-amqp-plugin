package org.apache.flume.amqp;

import java.util.Map;

import org.apache.flume.Clock;
import org.apache.flume.Event;
import org.apache.flume.Source;
import org.apache.flume.amqp.AmqpSource.EventBatchDeliveryListener;
import org.apache.flume.instrumentation.SourceCounter;

import com.rabbitmq.client.QueueingConsumer.Delivery;

public class SFEventBatchDeliveryListener extends EventBatchDeliveryListener {

	public SFEventBatchDeliveryListener(Source source,
			SourceCounter sourceCounter, int sourceId,
			boolean useMessageTimestamp, Clock clock) {
		super(source, sourceCounter, sourceId, useMessageTimestamp, clock);
	}
	
	public SFEventBatchDeliveryListener(Source source,
			SourceCounter sourceCounter, int sourceId,
			boolean useMessageTimestamp) {
		super(source, sourceCounter, sourceId, useMessageTimestamp);
	}

	@Override
	public Event createEventFrom(Delivery delivery) {
		// TODO Auto-generated method stub
		Event event = super.createEventFrom(delivery);
		Map<String, String> headers = event.getHeaders();
		headers.put("foo", "bar");
		event.setHeaders(headers);
		return event;
	}
	
	

}
