package study.j2se.queue.delayed;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ExpiresItem<T> implements Delayed
{

	private T t;

	private long expireTime;

	private long removeTime;

	public ExpiresItem(T t, long expireTime)
	{
		super();
		this.t = t;
		this.expireTime = expireTime;
		this.removeTime = TimeUnit.MILLISECONDS.convert(expireTime, TimeUnit.MILLISECONDS) + System.currentTimeMillis();
	}

	public int compareTo(Delayed o)
	{

		if (o == null)
			return 1;
		if (o == this)
			return 0;
		if (o instanceof ExpiresItem)
		{
			@SuppressWarnings("unchecked")
			ExpiresItem<T> tmpDelayedItem = (ExpiresItem<T>) o;
			if (expireTime > tmpDelayedItem.expireTime)
			{
				return 1;
			} else if (expireTime == tmpDelayedItem.expireTime)
			{
				return 0;
			} else
			{
				return -1;
			}
		}
		long diff = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
		return diff > 0 ? 1 : diff == 0 ? 0 : -1;
	}

	public T getT()
	{
		return t;
	}

	public void setT(T t)
	{
		this.t = t;
	}

	public long getDelay(TimeUnit unit)
	{

		return unit.convert(removeTime - System.currentTimeMillis(), unit);
	}

}
