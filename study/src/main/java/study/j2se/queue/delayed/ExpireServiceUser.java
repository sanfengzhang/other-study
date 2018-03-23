package study.j2se.queue.delayed;

import java.util.concurrent.ExecutorService;

public class ExpireServiceUser extends ExpireService<study.j2se.queue.delayed.User>
{

	public ExpireServiceUser(ExecutorService executorService, int size)
	{
		super(executorService, size);

	}

	@Override
	protected Runnable getRunnable(final User t)
	{
		Runnable r = new Runnable()
		{

			public void run()
			{
				System.out.println(t.toString());

			}
		};
		return r;
	}

}
