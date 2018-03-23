package study.j2se.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.junit.Test;

public class ThreadpoolTest
{

	@Test
	public void cachedThreadPoolTest()
	{

		final CountDownLatch latch = new CountDownLatch(100);
		ExecutorService executorService = Executors.newCachedThreadPool(new StudyThreadNameFactory());

		for (int i = 0; i < 100; i++)
		{
			Runnable r = new Runnable()
			{

				public void run()
				{
					System.out.println("id=" + Thread.currentThread().getId());
					latch.countDown();

				}
			};

			executorService.submit(r);

		}
		try
		{
			latch.await();
		} catch (InterruptedException e)
		{

			e.printStackTrace();
		}

	}

	public static class StudyThreadNameFactory implements ThreadFactory
	{

		public Thread newThread(Runnable r)
		{

			return new Thread(r);
		}
	}

}
