package com.escaf.flume.interceptor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EscafFlumeExpanInfoInterceptor implements Interceptor
{

	private String timestampHeaderKey;

	private String ipHeaderKey;

	private String hostnameHeaderKey;

	private String logTypeHeaderKey;

	private String ip = null;

	private String host = null;

	private String logTypeHeaderValue;

	private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	private static final Logger logger = LoggerFactory.getLogger(EscafFlumeExpanInfoInterceptor.class);

	public EscafFlumeExpanInfoInterceptor()
	{
		InetAddress addr;
		try
		{
			addr = InetAddress.getLocalHost();

			ip = addr.getHostAddress();

			host = addr.getCanonicalHostName();

		} catch (UnknownHostException e)
		{
			logger.warn("Could not get local host address. Exception follows.", e);
		}

	}

	public EscafFlumeExpanInfoInterceptor(String timestampHeaderKey, String ipHeaderKey, String hostnameHeaderKey,
			String logTypeHeaderKey, String logTypeHeaderValue)
	{
		super();
		this.timestampHeaderKey = timestampHeaderKey;
		this.ipHeaderKey = ipHeaderKey;
		this.hostnameHeaderKey = hostnameHeaderKey;
		this.logTypeHeaderKey = logTypeHeaderKey;
		this.logTypeHeaderValue = logTypeHeaderValue;

	}

	public String getTimestampHeaderKey()
	{
		return timestampHeaderKey;
	}

	public void setTimestampHeaderKey(String timestampHeaderKey)
	{
		this.timestampHeaderKey = timestampHeaderKey;
	}

	public String getIpHeaderKey()
	{
		return ipHeaderKey;
	}

	public void setIpHeaderKey(String ipHeaderKey)
	{
		this.ipHeaderKey = ipHeaderKey;
	}

	public String getHostnameHeaderKey()
	{
		return hostnameHeaderKey;
	}

	public void setHostnameHeaderKey(String hostnameHeaderKey)
	{
		this.hostnameHeaderKey = hostnameHeaderKey;
	}

	public String getLogTypeHeaderKey()
	{
		return logTypeHeaderKey;
	}

	public void setLogTypeHeaderKey(String logTypeHeaderKey)
	{
		this.logTypeHeaderKey = logTypeHeaderKey;
	}

	public String getLogTypeHeaderValue()
	{
		return logTypeHeaderValue;
	}

	public void setLogTypeHeaderValue(String logTypeHeaderValue)
	{
		this.logTypeHeaderValue = logTypeHeaderValue;
	}

	public void close()
	{

	}

	public void initialize()
	{

	}

	public Event intercept(Event event)
	{

		Map<String, String> headers = event.getHeaders();
		if (headers.containsKey(timestampHeaderKey))
		{

		} else
		{
			String dateStr = DateFormatUtils.format(new Date(), DATE_PATTERN);
			headers.put(timestampHeaderKey, dateStr);
		}
		if (headers.containsKey(hostnameHeaderKey))
		{
		} else
		{
			headers.put(hostnameHeaderKey, host);
		}
		if (headers.containsKey(ipHeaderKey))
		{

		} else
		{
			headers.put(ipHeaderKey, ip);
		}
		if (headers.containsKey(logTypeHeaderKey))
		{
		} else
		{
			headers.put(logTypeHeaderKey, logTypeHeaderValue);
		}

		return event;
	}

	public List<Event> intercept(List<Event> events)
	{

		for (Event event : events)
		{
			intercept(event);
		}
		return events;
	}

	public static class Builder implements Interceptor.Builder
	{

		private String timestampHeader;

		private String ipHeader;

		private String hostHeader;

		private String logtypeHeader;

		private String logTypeHeaderValue;

		public Interceptor build()
		{
			EscafFlumeExpanInfoInterceptor escafFlumeInterceptor = new EscafFlumeExpanInfoInterceptor();
			escafFlumeInterceptor.setHostnameHeaderKey(hostHeader);
			escafFlumeInterceptor.setIpHeaderKey(ipHeader);
			escafFlumeInterceptor.setTimestampHeaderKey(timestampHeader);
			escafFlumeInterceptor.setLogTypeHeaderKey(logtypeHeader);
			escafFlumeInterceptor.setLogTypeHeaderValue(logTypeHeaderValue);

			return escafFlumeInterceptor;
		}

		public void configure(Context context)
		{
			timestampHeader = context.getString(Constants.TIMESTAMP_HEADER_NAME,
					Constants.DEFAULT_TIMESTAMP_HEADER_NAME);
			ipHeader = context.getString(Constants.IP_HEADER_NAME, Constants.DEFAULT_IP_HEADER_NAME);

			hostHeader = context.getString(Constants.HOSTNAME_HEADER_NAME, Constants.DEFAULT_HOSTNAME_HEADER_NAME);

			logtypeHeader = context.getString(Constants.LOG_TYPE_HEADER_NAME, Constants.DEFAULT_LOG_TYPE_HEADER_NAME);

			logTypeHeaderValue = context.getString(Constants.LOG_TYPE_HEADER_VALUE_NAME,
					Constants.DEFAULT_LOG_TYPE_HEADER_VALUE_NAME);

		}

	}

}
