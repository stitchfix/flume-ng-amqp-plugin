package org.apache.flume.amqp;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Clock;
import org.apache.flume.Event;
import org.apache.flume.Source;
import org.apache.flume.amqp.AmqpSource.EventBatchDeliveryListener;
import org.apache.flume.instrumentation.SourceCounter;

import com.rabbitmq.client.QueueingConsumer.Delivery;

public class SFEventBatchDeliveryListener extends EventBatchDeliveryListener {

	private static final Log LOG = LogFactory.getLog(SFEventBatchDeliveryListener.class);

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
		/** add additional metadata to event**/
		Event event = super.createEventFrom(delivery);

		String actionHeader = null;
		String actionIDHeader = null;
		String lowerDomainHeader = null;
		String topDomainHeader = null;
		String modelHeader = null;
		
		//will be set to the reason if routing key is malformed
		String malformed = "";
		
		//get headers of current log
		Map<String, String> currentHeader = event.getHeaders();
		
		//pull the routingKey from the current log
		String currentRoutingKey = currentHeader.get("routingKey");
		
		//split the routingKey into individual parts
		//RK format: topDomain.lowerDomain.model(cant be multiple .s).actionId.action
		//sample: sf.merch.style.123.created
		//topDomain.lowerDomain.model.actionID.action
		String [] RKparts = currentRoutingKey.split("\\.");
		
		//if this is a valid routing key based on number of segments
		if(RKparts.length >= 5)
		{
			actionHeader = RKparts[RKparts.length-1];
			actionIDHeader = RKparts[RKparts.length-2];
			lowerDomainHeader = RKparts[1];
			topDomainHeader = RKparts[0];
			
			//since the model can span multiple segments, we must do this logic and concat
			//the parts with a "-" to avoid using "." with DB tables
			modelHeader="";
			for(int i = 2; i < RKparts.length - 2; i++)
			{
				//only do if > 2 as this means we will have a new part of model to append 
				if(i>2)
					modelHeader+="-";
				modelHeader+=RKparts[i];
			}
		}
		else
			malformed += "SFEventBatchDeliveryListener.createEventFrom: Insignficant segment length ";
		//add new headers 
		currentHeader.put("action", actionHeader);
		currentHeader.put("actionID", actionIDHeader);
		currentHeader.put("lowerDomain", lowerDomainHeader);
		currentHeader.put("topDomain", topDomainHeader);
		currentHeader.put("model", modelHeader);
		currentHeader.put("malformed", malformed+"");
		
		//check to see if timestamp is valid, and if not set it to current time
		if(!StringUtils.isNumeric(currentHeader.get("timestamp")))
			currentHeader.put("timestamp",  Long.toString(System.currentTimeMillis()));

		//add new headers (in combinatino with previous ones) to current event
		event.setHeaders(currentHeader);
		return event;
	}
	
	

}
