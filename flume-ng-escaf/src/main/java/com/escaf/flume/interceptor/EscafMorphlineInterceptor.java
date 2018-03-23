package com.escaf.flume.interceptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

public class EscafMorphlineInterceptor implements Interceptor
{

	private final Context context;

	private final EscafMorphlineHandler escafMorphlineHandler;

	private final Collector collector;

	public EscafMorphlineInterceptor(Context context)
	{
		this.context = context;
		escafMorphlineHandler = new EscafMorphlineHandler();
		this.collector = new Collector();
		this.escafMorphlineHandler.setFinalChild(collector);
		this.escafMorphlineHandler.configure(this.context);
	}

	public void initialize()
	{

	}

	public Event intercept(Event event)
	{

		collector.reset();
		escafMorphlineHandler.process(event);
		List<Record> results = collector.getRecords();
		if (results.size() == 0)
		{
			return null;
		}
		if (results.size() > 1)
		{
			throw new FlumeException(
					getClass().getName() + " must not generate more than one output record per input event");
		}
		Event result = toEvent(results.get(0));
		return result;
	}

	private Event toEvent(Record record)
	{
		Map<String, String> headers = new HashMap<String, String>();
		Map<String, Collection<Object>> recordMap = record.getFields().asMap();
		byte[] body = null;
		for (Map.Entry<String, Collection<Object>> entry : recordMap.entrySet())
		{
			if (entry.getValue().size() > 1)
			{
				throw new FlumeException(
						getClass().getName() + " must not generate more than one output value per record field");
			}
			assert entry.getValue().size() != 0;
			Object firstValue = entry.getValue().iterator().next();
			if (Fields.ATTACHMENT_BODY.equals(entry.getKey()))
			{
				if (firstValue instanceof byte[])
				{
					body = (byte[]) firstValue;
				} else if (firstValue instanceof InputStream)
				{
					try
					{
						body = ByteStreams.toByteArray((InputStream) firstValue);
					} catch (IOException e)
					{
						throw new FlumeException(e);
					}
				} else
				{
					throw new FlumeException(getClass().getName()
							+ " must non generate attachments that are not a byte[] or InputStream");
				}
			} else
			{
				headers.put(entry.getKey(), firstValue.toString());
			}
		}
		return EventBuilder.withBody(body, headers);
	}

	public List<Event> intercept(List<Event> events)
	{

		List<Event> results = new ArrayList<Event>(events.size());
		for (Event event : events)
		{
			event = intercept(event);
			if (event != null)
			{
				results.add(event);
			}
		}
		return results;
	}

	public void close()
	{

		this.escafMorphlineHandler.stop();

	}

	public static class Builder implements Interceptor.Builder
	{

		private Context context;

		public Builder()
		{
		}

		public EscafMorphlineInterceptor build()
		{
			return new EscafMorphlineInterceptor(context);
		}

		public void configure(Context context)
		{
			this.context = context;
		}

	}

	public static final class Collector implements Command
	{

		private final List<Record> results = new ArrayList<Record>();

		public List<Record> getRecords()
		{
			return results;
		}

		public void reset()
		{
			results.clear();
		}

		public Command getParent()
		{
			return null;
		}

		public void notify(Record notification)
		{
		}

		public boolean process(Record record)
		{
			Preconditions.checkNotNull(record);
			results.add(record);
			return true;
		}

	}
}
