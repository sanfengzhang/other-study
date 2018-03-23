package study.j2se.queue.delayed;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;

public abstract class ExpireService<T>
{

	DelayQueue<ExpiresItem<T>> delayQueue = new DelayQueue<ExpiresItem<T>>();

	private int size;

	private ExecutorService executorService;

	public ExpireService(ExecutorService executorService, int size)
	{
		this.executorService = executorService;
		this.size = size;
		new Thread()
		{
			public void run()
			{

				execute();
			}

		}.start();

	}

	public void add(T t, long expireTime)
	{
		if (delayQueue.size() >= size)
		{
			throw new RuntimeException("size of queue must less than " + size);
		}

		if (null == t)
		{
			throw new NullPointerException("value must not be null.");

		}

		ExpiresItem<T> expiresItem = new ExpiresItem<T>(t, expireTime);

		delayQueue.add(expiresItem);
	}

	/** 这里主要针对过期的对象封装成runnable对象返回,并提交到线程池，可以执行用户需要在对象过期时候想执行的操作 */
	protected abstract Runnable getRunnable(T t);

	public void execute()
	{

		while (true)
		{
			try
			{
				ExpiresItem<T> item = delayQueue.take();
				T t = item.getT();
				executorService.submit(getRunnable(t));
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
