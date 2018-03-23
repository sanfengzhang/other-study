package study.j2se.queue.delayed;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class DelayQueueTest
{

	@Test
	public void expireRunnableTest() throws IOException
	{

		User u1 = new User(1, "zhangsan");
		User u2 = new User(2, "lisi");		

		ExpireServiceUser expireService = new ExpireServiceUser(Executors.newFixedThreadPool(3), 10);
		expireService.add(u1, 3000L);
		expireService.add(u2, 5000L);
		
		System.in.read();

	}

	@Test
	public void expireTest() throws InterruptedException
	{
		ExpireCache<String, Integer> expire = new ExpireCache<String, Integer>(20);
		expire.add("zhangsan", 10, 3000);

		expire.add("lisi", 20, 5000);
		expire.add("wangwu", 30, 10000);

		Thread.sleep(3000L);

		System.out.println(expire.get("zhangsan"));
		System.out.println(expire.get("lisi"));
		System.out.println(expire.get("wangwu"));

	}

	@Test
	public void delayQueueTest() throws InterruptedException
	{
		final BlockingQueue<DelayedObject> queue = new DelayQueue<DelayedObject>();
		final long cu = System.currentTimeMillis();

		queue.add(new DelayedObject("zhangsan", cu + 3000));

		queue.add(new DelayedObject("zhangsan1", cu + 5000));

		new Thread()
		{
			public void run()
			{
				try
				{
					Thread.sleep(15000);
				} catch (InterruptedException e)
				{

					e.printStackTrace();
				}
				queue.add(new DelayedObject("zhangsan2", cu + 10000));
			}
		}.start();

		while (true)
		{
			long start = System.currentTimeMillis();
			System.out.println(queue.take().getId());
			long end = System.currentTimeMillis();
			System.out.println(end - start);
		}

	}

	public static class UserTask implements Runnable
	{
		private User user;

		public UserTask(User user)
		{
			super();
			this.user = user;
		}

		public void run()
		{
			System.out.println(user.toString());

		}
	}

	public static class DelayedObject implements Delayed
	{
		private long delay;

		private String id;

		public DelayedObject(String id, long delay)
		{
			this.delay = delay;
			this.id = id;
		}

		public int compareTo(Delayed o)
		{
			if (o instanceof DelayedObject)
			{
				DelayedObject delayedObject = (DelayedObject) o;

				return this.id.compareTo(delayedObject.getId());
			}

			return 0;
		}

		public String getId()
		{
			return id;
		}

		public long getDelay(TimeUnit unit)
		{

			return unit.convert(delay - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		}
	}

}
