package org.apache.flume.amqp;

import java.util.Arrays;
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

	String [] RKBypass = {"sf.item.event", "sf.shipment.event", "sf.tracking_numbers.updated",
						  "sf.admin_users.measurement.shipments_processed_by_returns,",
						  "sf.admin_users.measurement.items_physically_processed_during_returns",
						  "sf.transactions.completed"};
	
	@Override
	public Event createEventFrom(Delivery delivery) {
		/** add additional metadata to event**/
		Event event = super.createEventFrom(delivery);

		String topDomainHeader = null;
		String lowerDomainHeader = null;
		String modelHeader = null;
		String actionIDHeader = null;
		String actionHeader = null;
		
		//will be set to the reason if routing key is malformed
		String malformed = "";
		
		//get headers of current log
		Map<String, String> currentHeader = event.getHeaders();
		
		//pull the routingKey from the current log
		String currentRoutingKey = currentHeader.get("routingKey");
		
		//split the routingKey into individual parts
		//RK format: topDomain.lowerDomain.model(can be multiple .'s).actionId.action
		//sample: sf.merch.style.123.created
		//topDomain.lowerDomain.model.actionID.action
		String [] RKParts = currentRoutingKey.split("\\.");
		int RKPartsLength = RKParts.length;
		
		/*if this is a valid routing key based on the following logic:
		* if it have >=5 segments
		* it its >=3 segments and in the rkBypass array
		*/
		if(RKPartsLength >= 5)
		{
			topDomainHeader = RKParts[0];
			lowerDomainHeader = RKParts[1];
			actionIDHeader = RKParts[RKParts.length-2];
			actionHeader = RKParts[RKParts.length-1];
			
			//since the model can span multiple segments, we must do this logic and concat
			//the parts with a "-" to avoid using "." with DB tables
			modelHeader="";
			for(int i = 2; i < RKParts.length - 2; i++)
			{
				//only do if > 2 as this means we will have a new part of model to append 
				if(i>2)
					modelHeader+="-";
				
				modelHeader+=RKParts[i];
			}
		}
		else if(RKPartsLength == 4)
		{
			topDomainHeader = RKParts[0];
			lowerDomainHeader = RKParts[1];
			modelHeader = RKParts[2];
			actionIDHeader = RKParts[3];
		}
		else if(RKPartsLength == 3)
		{
			topDomainHeader = RKParts[0];
			lowerDomainHeader = RKParts[1];
			modelHeader = RKParts[2];
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
		currentHeader.put("routingKey", currentRoutingKey);
		
		//check to see if timestamp is valid, and if not set it to current time
		if(!StringUtils.isNumeric(currentHeader.get("timestamp")))
			currentHeader.put("timestamp",  Long.toString(System.currentTimeMillis()));

		//add new headers (in combination with previous ones) to current event
		event.setHeaders(currentHeader);
		return event;
	}
	
	

}
