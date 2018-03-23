package study.j2se.queue.delayed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

public class ExpireCache<K, V>
{
	DelayQueue<ExpiresItem<K>> delayQueue = new DelayQueue<ExpiresItem<K>>();

	Map<K, V> expireCache = null;

	private int size;

	public ExpireCache(int size)
	{

		this.size = size;
		expireCache = new ConcurrentHashMap<K, V>(size);
		expire();
	}

	public void add(K k, V v, long expireTime)
	{
		if (delayQueue.size() >= size)
		{
			throw new RuntimeException("size of queue must less than " + size);
		}

		if (null == v)
		{
			throw new NullPointerException("value must not be null.");

		}

		ExpiresItem<K> expiresItem = new ExpiresItem<K>(k, expireTime);

		delayQueue.add(expiresItem);
		expireCache.put(k, v);
	}

	public V get(K k)
	{
		return expireCache.get(k);

	}

	private void expire()
	{
		Runnable r = new ExpireRunnable();
		Thread removeThread = new Thread(r);
		removeThread.start();
	}

	private class ExpireRunnable implements Runnable
	{

		public void run()
		{
			while (true)
			{
				try
				{
					ExpiresItem<K> item = delayQueue.take();
					K k = item.getT();
					expireCache.remove(k);
				} catch (InterruptedException e)
				{
					try
					{
						Thread.sleep(100);
					} catch (InterruptedException e1)
					{
						e1.printStackTrace();
					}
				}
			}

		}

	}
}
