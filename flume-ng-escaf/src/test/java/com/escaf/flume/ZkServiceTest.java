package com.escaf.flume;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;

import com.codahale.metrics.SharedMetricRegistries;
import com.escaf.flume.interceptor.EscafMorphlineInterceptor;
import com.escaf.flume.interceptor.EscafMorphlineInterceptor.Collector;
import com.escaf.flume.support.ZkService;
import com.typesafe.config.ConfigFactory;

public class ZkServiceTest
{

	@Test
	public void deleteTest() throws Exception
	{
		CuratorFramework client = ZkService.createSimpleClient("192.168.1.100:2181");
		client.start();

		client.delete().deletingChildrenIfNeeded().forPath("/morphline");
	}

	@Test
	public void createPath() throws Exception
	{
		CuratorFramework client = ZkService.createSimpleClient("192.168.1.100:2181");
		client.start();
		client.create().forPath("/morphline");
		client.create().forPath("/morphline/collector");
	}

	@Test
	public void createTranslog() throws Exception
	{
		CuratorFramework client = ZkService.createSimpleClient("192.168.1.100:2181");
		client.start();
		String morphlineParentPath = "/morphline/collector";

		if (client.checkExists().forPath(morphlineParentPath) == null)
		{
			client.create().forPath("/morphline");
			client.create().forPath(morphlineParentPath);
		}

		File file = new File("F:\\workspace\\flume\\flume-ng-escaf\\src\\test\\resources\\transgrok.conf");
		client.create().forPath(morphlineParentPath + "/trans_log",
				IOUtils.toString(new FileInputStream(file)).getBytes());
	}

	@Test
	public void createEventlog() throws Exception
	{
		CuratorFramework client = ZkService.createSimpleClient("192.168.1.100:2181");
		client.start();
		String morphlineParentPath = "/morphline/collector";

		if (client.checkExists().forPath(morphlineParentPath) == null)
		{
			client.create().forPath("/morphline");
			client.create().forPath(morphlineParentPath);
		}

		File file = new File("F:\\workspace\\flume\\flume-ng-escaf\\src\\test\\resources\\transgrok2.conf");
		client.create().forPath(morphlineParentPath + "/event_log",
				IOUtils.toString(new FileInputStream(file)).getBytes());
	}

	@Test
	public void morphlineZkConfigTest() throws Exception
	{

		Map<String, Command> morphlineMap = new ConcurrentHashMap<String, Command>();
		FaultTolerance faultTolerance = new FaultTolerance(false, false);

		MorphlineContext morphlineContext = new MorphlineContext.Builder().setExceptionHandler(faultTolerance)
				.setMetricRegistry(SharedMetricRegistries.getOrCreate("testId")).build();
		Collector finalChild = new EscafMorphlineInterceptor.Collector();

		CuratorFramework client = ZkService.createSimpleClient("192.168.1.100:2181");
		client.start();
		String morphlineParentPath = "/morphline/collector";

		if (client.checkExists().forPath(morphlineParentPath) == null)
		{
			client.create().forPath("/morphline");
			client.create().forPath(morphlineParentPath);
		}

		File file = new File("F:\\workspace\\flume\\flume-ng-escaf\\src\\test\\resources\\transgrok.conf");
		client.create().forPath(morphlineParentPath + "/trans_log",
				IOUtils.toString(new FileInputStream(file)).getBytes());

		List<String> childPaths = client.getChildren().forPath(morphlineParentPath);
		for (String path : childPaths)
		{
			String childPath = morphlineParentPath + "/" + path;
			//
			// String morphlineData = new String(client.getData().forPath(childPath),
			// "UTF-8");
			// Config morphlineConfig = ConfigFactory.parseResources(morphlineData);
			// morphlineConfig = ConfigFactory.load(morphlineConfig);
			// morphlineConfig.checkValid(ConfigFactory.defaultReference());
			// System.out.println("----------" + new Configs().getString(morphlineConfig,
			// "id"));
			//
			// Command command = new PipeBuilder().build(morphlineConfig, null, finalChild,
			// morphlineContext);
			// morphlineMap.put(path, command);

			System.out.println(FileUtils.getTempDirectoryPath());
			File f = new File(FileUtils.getTempDirectoryPath() + "/grok.temp");
			IOUtils.write(client.getData().forPath(childPath), new FileOutputStream(f));
			Command command = new Compiler().compile(f, "trans_log", morphlineContext, finalChild,
					ConfigFactory.empty());
			morphlineMap.put(path, command);
			f.delete();

		}

		Record record = new Record();
		record.put(Fields.ATTACHMENT_BODY, "2017-11-27|751509".getBytes());
		morphlineMap.get("trans_log").process(record);

		System.out.println(finalChild.getRecords().get(0));

	}

	@Test
	public void readTest() throws Exception
	{

		CuratorFramework client = ZkService.createSimpleClient("121.201.78.13:2181");

		client.start();

		List<String> childPaths = client.getChildren().forPath("/flume");

		for (String path : childPaths)
		{
			System.out.println(new String(client.getData().forPath("/flume/" + path)));
		}
	}

	@Test
	public void zkCreateTest() throws Exception
	{

		CuratorFramework client = ZkService.createSimpleClient("121.201.78.13:2181");

		client.start();

		if (client.checkExists().forPath("/morphline") != null)
		{
			client.delete().deletingChildrenIfNeeded().forPath("/morphline");
		}
		client.create().forPath("/morphline");
		client.create().forPath("/morphline/collector1");

		Watcher watcher = new Watcher()
		{
			public void process(WatchedEvent event)
			{
				EventType eventType = event.getType();
				if (EventType.NodeChildrenChanged == eventType)
				{

					System.out.println(event.getType().name());

				}

			}
		};

		ZkService.watchedGetChildren(client, "/morphline/collector1", watcher);

		System.in.read();
	}

	@Test
	public void childEventTest() throws Exception
	{
		CuratorFramework client = ZkService.createSimpleClient("121.201.78.13:2181");

		client.start();

		if (client.checkExists().forPath("/morphline") != null)
		{
			client.delete().deletingChildrenIfNeeded().forPath("/morphline");
		}
		client.create().forPath("/morphline");
		client.create().forPath("/morphline/collector1");

		ZkService.watchedPathChildrenCacheListener(client, "/morphline/collector1", new PathChildrenCacheListener()
		{
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
			{
				if (null != event)
				{
					System.out.println(event.getType());
					if (null != event.getData())
					{
						String chidcontent = new String(event.getData().getData());
						System.out.println(chidcontent);

					}
				}

			}
		});
		System.in.read();
	}

	@Test
	public void addChidren() throws Exception
	{

		CuratorFramework client = ZkService.createSimpleClient("121.201.78.13:2181");
		client.start();
		ZkService.create(client, "/morphline/collector1/trans_log", "test trans log".getBytes());

		client.close();
	}

}
