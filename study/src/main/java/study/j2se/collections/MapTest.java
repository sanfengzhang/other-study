package study.j2se.collections;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.junit.Test;

public class MapTest
{

	final ConcurrentMap<String, Future<User>> cache = new ConcurrentHashMap<String, Future<User>>();

	/**
	 * Map是一种Hash表数据结构是由一个线性表和链表或红黑树组成的。这种数据结构的原因是检索数据块，因为在不考虑hash冲突时候线性表查找元素的速度O(1),
	 * hash表示同过函数将关键字通过某种算法将其映射到数组的某个位置。 1.HashMap是怎样想表中插入数据的？ 插入数据发生了Hash碰撞怎么解决？
	 * 插入时容量满了怎么办？ HashMap中什么样的对象非常适合作为key？
	 * 
	 * {@ HashMap}是非线程安全类 tab[i = (n - 1) & hash])这个是计算关键字在线性表中的位置,为什么是n-1而不是n?
	 * hashMap的负载因子、双倍扩容、扩容时会重新计算所有的key的index.所以对Map容量初始化有一个较好的预估很重要
	 * 
	 * 这个是不可变的对象,且任意的两个对象能返回不同的hashCode
	 * 
	 * {@ LinkedHashMap}是集成了HashMap但是它实现了保留在添加键值对时候的顺序，这里面使用指针来维护顺序关系、
	 * 
	 * {@ ConcurrentHashMap}的作用?
	 * 因为在某些并发的环境下想使用Map，而传统的hashTable性能实在太差，每一次写操作都要加锁。那么有没有提升性能的方法呢?
	 * concurrentHashMap就给出了一种解决方法，将Map分为若干的segment,去降低加锁的的粒度。
	 * 使用concurrentHashMap结合Future的应用场景.eg:例如多个线程同时创建一个共享对象，但是最终只需要一个对象，如何保证创建的对象的唯一性呢？
	 * 很重要的特性就是使用puIfAbsent方法的特性,和futureTask的异步执行的方法
	 * 
	 * {@ TreeMap}有序的hash表，但是费线程安全的。基于红黑树的实现
	 * 
	 * {@ ConcurrentSkipListMap} 线程安全的有序的Hash表，实现是基于跳表完成的。程序效率一般解决的方式有：时间换空间或者空间换时间
	 * {@ ConcurrentSkipListMap}这个效率设计是基于空间换时间设计而成的
	 * 
	 * 在做数据查询的算法的时候有二分查找和AVL树查找。
	 * 因为在基于红黑树的实现相对复杂，但是基于跳表实现就简单一些；所谓跳表就是对一组数据集进行排好序之后，在这一层表之上根据情况再构建多层表，
	 * 第一层是包含所有数据集，第二层在第一层的基础上做
	 * 
	 * 
	 */
	@Test
	public void linkedHashMapTest()
	{

		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(2, "a");
		map.put(0, "a");
		map.put(-1, "a");

		Map<Integer, String> map1 = new LinkedHashMap<Integer, String>();
		map1.put(2, "a");
		map1.put(0, "a");
		map1.put(-1, "a");

		Map<Integer, String> map2 = new TreeMap<Integer, String>();
		map2.put(2, "a");
		map2.put(0, "a");
		map2.put(-1, "a");

		System.out.println(map.toString());
		System.out.println(map1.toString());
		System.out.println(map2.toString());

	}

	@Test
	public void putIfAbsentTest()
	{
		final ConcurrentMap<String, String> cache1 = new ConcurrentHashMap<String, String>();
		String result = cache1.putIfAbsent("a", "1");
		System.out.println(result);
		result = cache1.putIfAbsent("a", "2");
		System.out.println(result);

		result = cache1.putIfAbsent("a", "3");
		System.out.println(result);
	}

	/**
	 * 线程安全且有序的Hash表
	 */
	@Test
	public void concurrentSkipListMapTest()
	{

		Map<Integer, String> concurrentSkipListMap = new ConcurrentSkipListMap<Integer, String>();

		concurrentSkipListMap.put(2, "a");
		concurrentSkipListMap.put(0, "a");
		concurrentSkipListMap.put(-1, "a");
		
		System.out.println(concurrentSkipListMap.toString());

	}

	@Test
	public void concurrentHashMapAndFutureTest()
	{
		try
		{
			testCache("zhangsan");
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		} catch (ExecutionException e)
		{
			e.printStackTrace();
		}

	}

	private User testCache(final String name) throws InterruptedException, ExecutionException
	{
		Future<User> future = cache.get(name);

		if (null == future)
		{
			FutureTask<User> futureTask = new FutureTask<MapTest.User>(new Callable<User>()
			{

				public User call() throws Exception
				{
					User user = new User();
					user.setName(name);
					return user;
				}
			});

			// 比如有多个线程同时创建zhangsan这个User,如果已经存在对应的key则返回对应的值，
			// 如果不存在对应的key则返回null.
			future = cache.putIfAbsent(name, future);

			// future取到为空的时候
			if (null == future)
			{
				futureTask.run();
			}
		}

		return future.get();

	}

	class User
	{
		int id;

		String name;

		public int getId()
		{
			return id;
		}

		public void setId(int id)
		{
			this.id = id;
		}

		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

	}

}
