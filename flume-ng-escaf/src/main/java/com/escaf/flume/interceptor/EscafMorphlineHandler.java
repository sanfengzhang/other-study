package com.escaf.flume.interceptor;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.escaf.flume.support.ZkService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 1.设计说明,这个扩展的morphline是需要支持多个不同数据类型的正则解析或者ETL操作的 在使用{@
 * EscafFlumeExpanInfoInterceptor}已经将每条日志的类型加入到数据中，所以可以根据日志类型
 * 字段获取对应的morphline解析对象对数据进行解析 2.根据interceptor是配置在数据源的一侧，这个是很重要的！所以在flume
 * master一侧使用该扩展类没有什么问题，只有一个source。 如果存在多个source
 * 
 * @author owner
 * 
 *
 */
public class EscafMorphlineHandler {

	private MorphlineContext morphlineContext;

	/** 这个对象应该是比较重量级的 */
	private Map<String, Command> morphlineMap = new ConcurrentHashMap<String, Command>();

	private Command finalChild;
	private String morphlineFileAndId;

	private Timer mappingTimer;
	private Meter numRecords;
	private Meter numFailedRecords;
	private Meter numExceptionRecords;

	private String logTypeHeaderKey;

	public static final String ESCAF_MORPHLINE_FILE_PARAM = "escafMorphlineFile";

	public static final String ESCAF_MORPHLINE_ID_PARAM = "escafMorphlineId";

	public static final String ESCAF_MORPHLINE_VARIABLE_PARAM = "escafMorphlineVariable";

	public static final String ESCAF_MORPHLINE_ALL_ID_PARAM = "escafAllLogType";

	public static final String DEFAULT_ESCAF_LOG_TYPE_HEADER_KEY = "escaf_logtype";

	public static final String ESCAF_MORPHLINE_FILE_ZK_PARAM = "escafMorphlineZk";

	public static final String ESCAF_MORPHLINE_FILE_ZK_PARAM_PATH = "escafMorphlineZkFilePath";

	// 文件夹的位置
	public static final String ESCAF_MORPHLINE_DICTIONARYFILES = "escafMorphlineDictionaryFiles";

	// 类路径
	public static final String ESCAF_MORPHLINE_DICTIONARYSOURCE = "escafMorphlineDictionarySource";

	private String escafMorphlineDictionaryFiles = null;

	// private String escafMorphlineDictionarySource = null;

	private static final Logger LOG = LoggerFactory.getLogger(EscafMorphlineHandler.class);

	private CuratorFramework client = null;

	void setFinalChild(Command finalChild) {
		this.finalChild = finalChild;
	}

	public void configure(Context context) {

		String morphlineFile = context.getString(ESCAF_MORPHLINE_FILE_PARAM);
		String morphlineId = context.getString(ESCAF_MORPHLINE_ID_PARAM);
		logTypeHeaderKey = context.getString(Constants.LOG_TYPE_HEADER_NAME, DEFAULT_ESCAF_LOG_TYPE_HEADER_KEY);
		morphlineFileAndId = morphlineFile + "@" + morphlineId;

		if (morphlineContext == null) {
			FaultTolerance faultTolerance = new FaultTolerance(
					context.getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false),
					context.getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
					context.getString(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES));

			morphlineContext = new MorphlineContext.Builder().setExceptionHandler(faultTolerance)
					.setMetricRegistry(SharedMetricRegistries.getOrCreate(morphlineFileAndId)).build();
		}
		Config override = ConfigFactory.parseMap(context.getSubProperties(ESCAF_MORPHLINE_VARIABLE_PARAM + "."));

		String zkHost = context.getString(ESCAF_MORPHLINE_FILE_ZK_PARAM);

		// zk配置规则不需要指定有哪些logType,凡是在/morphline节点下的子节点均被初始化为解析规则，且节点的名称默认为morphlineId
		if (null != zkHost && !"".equals(zkHost)) {
			client = ZkService.createSimpleClient(zkHost);
			client.start();

			escafMorphlineDictionaryFiles = context.getString(ESCAF_MORPHLINE_DICTIONARYFILES);
			// escafMorphlineDictionarySource =
			// context.getString(ESCAF_MORPHLINE_DICTIONARYSOURCE,
			// "grok-dictionaries/firewalls,grok-dictionaries/grok-patterns");

			String morphlineParentPath = context.getString(ESCAF_MORPHLINE_FILE_ZK_PARAM_PATH);
			List<String> childPaths = new ArrayList<String>();
			try {
				childPaths = client.getChildren().forPath(morphlineParentPath);
				ZkService.watchedPathChildrenCacheListener(client, morphlineParentPath,
						new MorphlineCommandChangeListener());
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (String path : childPaths) {
				createCommand(morphlineParentPath + "/" + path, path, false);
			}

		} else {// 文件配置规则的形式,logType中存在的日志类型与文件中的morphlineId是一一对应的
			if (morphlineFile == null || morphlineFile.trim().length() == 0) {
				throw new MorphlineCompilationException("Missing parameter: " + ESCAF_MORPHLINE_FILE_PARAM, null);
			}

			String logTypesStr = context.getString(ESCAF_MORPHLINE_ALL_ID_PARAM);
			String[] logTypes = StringUtils.split(logTypesStr, ",");
			File file = new File(morphlineFile);
			for (String logType : logTypes) {
				Command morphline = new Compiler().compile(file, logType, morphlineContext, finalChild, override);
				morphlineMap.put(logType, morphline);
			}
		}

		this.mappingTimer = morphlineContext.getMetricRegistry()
				.timer(MetricRegistry.name("morphline.app", Metrics.ELAPSED_TIME));
		this.numRecords = morphlineContext.getMetricRegistry()
				.meter(MetricRegistry.name("morphline.app", Metrics.NUM_RECORDS));
		this.numFailedRecords = morphlineContext.getMetricRegistry()
				.meter(MetricRegistry.name("morphline.app", "numFailedRecords"));
		this.numExceptionRecords = morphlineContext.getMetricRegistry()
				.meter(MetricRegistry.name("morphline.app", "numExceptionRecords"));

	}

	public void process(Event event) {
		numRecords.mark();
		Timer.Context timerContext = mappingTimer.time();
		try {
			Record record = new Record();
			for (Entry<String, String> entry : event.getHeaders().entrySet()) {
				record.put(entry.getKey(), entry.getValue());
			}
			byte[] bytes = event.getBody();

			if (bytes != null && bytes.length > 0) {
				record.put(Fields.ATTACHMENT_BODY, bytes);
			}
			try {
				String logType = event.getHeaders().get(logTypeHeaderKey);

				Command morphline = morphlineMap.get(logType);
				if (null == morphline) {
					numFailedRecords.mark();
					LOG.warn("can not find correct command process record logtype={} Morphline={}", logType,
							morphlineFileAndId);
					return;
				}
				Notifications.notifyStartSession(morphline);
				if (!morphline.process(record)) {

					numFailedRecords.mark();
					LOG.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
				}
			} catch (RuntimeException t) {
				t.printStackTrace();
				numExceptionRecords.mark();
				morphlineContext.getExceptionHandler().handleException(t, record);
			}
		} finally {
			timerContext.stop();
		}
	}

	public void stop() {

		if (null != morphlineMap && morphlineMap.size() > 0) {
			for (Command c : morphlineMap.values()) {
				Notifications.notifyShutdown(c);
			}

			morphlineMap = null;
		}

		if (null != client) {
			client.close();
		}

	}

	private void createCommand(String path, String logType, boolean flag) {
		// XXX 这里没有弄清楚原因为什么初始化的时候会触发添加子节点事件!
		if (flag) {
			if (null != morphlineMap.get(logType)) {
				LOG.info("ADD CHILD BUT LOGYPE HAD EXIST,TYPE={}", logType);
				return;
			}

		}
		File file = null;
		FileOutputStream fos = null;
		String morphlineCfg = null;
		try {
			String fileName = UUID.randomUUID().toString();
			String filePath = FileUtils.getTempDirectoryPath() + "/" + fileName + ".tmp";
			file = new File(filePath);
			fos = new FileOutputStream(file);

			morphlineCfg = new String(client.getData().forPath(path), "utf-8");
			if (null == morphlineCfg || "".equals(morphlineCfg)) {
				LOG.warn("morphlineCfg null then return,type={},morphlineCfg={}", logType, morphlineCfg);
				return;
			}
			if (null != escafMorphlineDictionaryFiles) {
				morphlineCfg = morphlineCfg.replace("escafMorphlineDictionaryFiles", escafMorphlineDictionaryFiles);
			} else {
				morphlineCfg = morphlineCfg.replace("escafMorphlineDictionaryFiles", "");
			}

			morphlineCfg = morphlineCfg.replace("escafMorphlineDictionarySource", "");

			IOUtils.write(morphlineCfg.getBytes("UTF-8"), fos);
			Command morphline = new Compiler().compile(file, logType, morphlineContext, finalChild,
					ConfigFactory.empty());
			morphlineMap.put(logType, morphline);
			LOG.info("create command success,type={},morphlineCfg={}", logType, morphlineCfg);

		} catch (Exception e) {
			LOG.error("create commad failed logType={},morphlineCfg={}", logType, morphlineCfg);

		} finally {

			IOUtils.closeQuietly(fos);
			if (null != file) {
				file.delete();
			}

		}

	}

	private final class MorphlineCommandChangeListener implements PathChildrenCacheListener {

		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			if (null != event) {
				if (null != event.getData()) {
					String path = event.getData().getPath();
					if (path != null) {
						LOG.info("receieve zk event type={}", event.getType());
						int len = path.length();
						if (Type.CHILD_ADDED == event.getType()) {
							createCommand(path, path.substring(path.lastIndexOf("/") + 1, len), true);
						} else if (Type.CHILD_UPDATED == event.getType()) {
							LOG.info("update command type={}", path.substring(path.lastIndexOf("/") + 1, len));
							Command oldCmd = morphlineMap.remove(path);
							if (null != oldCmd) {
								Notifications.notifyShutdown(oldCmd);
							}

							createCommand(path, path.substring(path.lastIndexOf("/") + 1, len), false);

						} else if (Type.CHILD_REMOVED == event.getType()) {
							LOG.info("remove command type={}", path.substring(path.lastIndexOf("/") + 1, len));
							Command oldCmd = morphlineMap.remove(path);
							if(null!=oldCmd) {
								Notifications.notifyShutdown(oldCmd);
							}
							
						}
					}

				}

			}
		}

	}

}
