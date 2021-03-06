package com.escaf.flume.support;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

public class ZkService
{

	public static CuratorFramework createSimpleClient(String connectionString)
	{

		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

		return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
	}

	public static CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy,
			int connectionTimeoutMs, int sessionTimeoutMs)
	{

		return CuratorFrameworkFactory.builder().connectString(connectionString).retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs)

				.build();
	}

	public static void create(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		// this will create the given ZNode with the given data
		client.create().forPath(path, payload);
	}

	public static void createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		// this will create the given EPHEMERAL ZNode with the given data
		client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
	}

	public static String createEphemeralSequential(CuratorFramework client, String path, byte[] payload)
			throws Exception
	{
		// this will create the given EPHEMERAL-SEQUENTIAL ZNode with the given
		// data using Curator protection.
		return client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
	}

	public static void setData(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		// set data for the given node
		client.setData().forPath(path, payload);
	}

	public static void setDataAsync(CuratorFramework client, String path, byte[] payload) throws Exception
	{
		// this is one method of getting event/async notifications
		CuratorListener listener = new CuratorListener()
		{

			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
			{

			}
		};
		client.getCuratorListenable().addListener(listener);
		// set data for the given node asynchronously. The completion
		// notification
		// is done via the CuratorListener.
		client.setData().inBackground().forPath(path, payload);
	}

	public static void setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path,
			byte[] payload) throws Exception
	{
		// this is another method of getting notification of an async completion
		client.setData().inBackground(callback).forPath(path, payload);
	}

	public static void delete(CuratorFramework client, String path) throws Exception
	{
		// delete the given node
		client.delete().forPath(path);
	}

	public static void guaranteedDelete(CuratorFramework client, String path) throws Exception
	{
		// delete the given node and guarantee that it completes
		client.delete().guaranteed().forPath(path);
	}

	public static List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception
	{
		/**
		 * Get children and set a watcher on the node. The watcher notification will
		 * come through the CuratorListener (see setDataAsync() above).
		 */
		return client.getChildren().watched().forPath(path);
	}

	public static List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher)
			throws Exception
	{
		/**
		 * Get children and set the given watcher on the node.
		 */
		return client.getChildren().usingWatcher(watcher).forPath(path);
	}

	public static void watchedPathChildrenCacheListener(CuratorFramework client, String parentPath,
			PathChildrenCacheListener pathChildrenCacheListener) throws Exception
	{
		@SuppressWarnings("resource")
		PathChildrenCache pathChildrenCache = new PathChildrenCache(client, parentPath, true);
		pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
		pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
	}

}
