package com.escaf.flume;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.SharedMetricRegistries;
import com.escaf.flume.interceptor.EscafMorphlineInterceptor;
import com.escaf.flume.interceptor.EscafMorphlineInterceptor.Collector;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MorphlineTest {

	private MorphlineContext morphlineContext;

	private Command morphline;

	private Collector finalChid;

	@Test
	public void testSub() {
		String path = "morpline/collector/transl";
		System.out.println(path.substring(path.lastIndexOf("/")+1, path.length()));
	}

	@Test
	public void testCmd() throws IOException {
		Config override = ConfigFactory.empty();
		Compiler compiler = new Compiler();

		Config config = compiler.parse(
				new File("F:\\workspace\\flume\\flume-ng-escaf\\src\\test\\resources\\grokMyTranslog2.conf"), override);

		Set<Map.Entry<String, Object>> entries = config.root().unwrapped().entrySet();

		if (entries.size() != 1) {
			System.out.println("-----------------------------");
		}
		Map.Entry<String, Object> entry = entries.iterator().next();
		String cmdName = entry.getKey();

		Class cmdClass;

		if (!cmdName.contains(".") && !cmdName.contains("/")) {
			cmdClass = morphlineContext.getCommandBuilder(cmdName);
			if (cmdClass == null) {
				System.out.println("-----------------------------");
			}
		} else {
			String className = cmdName.replace('/', '.');
			try {
				cmdClass = Class.forName(className);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testGrokMy1() throws Exception {

		createMorphline("grokMyTranslog2.conf");
		long start = System.currentTimeMillis();

		for (int i = 0; i < 1; i++) {
			Record record = new Record();
			String msg = "Jun 10 04:42:51 INFO FLOW";
			record.put(Fields.MESSAGE, msg);
			System.out.println((morphline.process(record)));
			morphline.process(record);
			System.out.println(finalChid.getRecords().get(0));
			record = finalChid.getRecords().get(0);
			ListMultimap<String, Object> list = record.getFields();

			Map<String, Collection<Object>> map = list.asMap();

			Set<Entry<String, Collection<Object>>> set = map.entrySet();

			Iterator<Entry<String, Collection<Object>>> it = set.iterator();

			while (it.hasNext()) {
				Entry<String, Collection<Object>> en = it.next();

				System.out.println(en.getKey());
			}

		}

		long end = System.currentTimeMillis();
		System.out.println((end - start) / 1000);

	}

	@Test
	public void testConf() {
		createMorphline("test2.conf");
		Record record = new Record();
		String msg = "Jun 10 04:42:51 INFO FLOW";
		record.put(Fields.ATTACHMENT_BODY, msg.getBytes());

		Notifications.notifyStartSession(morphline);
		System.out.println((morphline.process(record)));

		System.out.println(finalChid.getRecords());
		record = finalChid.getRecords().get(0);
		ListMultimap<String, Object> list = record.getFields();

		Map<String, Collection<Object>> map = list.asMap();

		Set<Entry<String, Collection<Object>>> set = map.entrySet();

		Iterator<Entry<String, Collection<Object>>> it = set.iterator();

		while (it.hasNext()) {
			Entry<String, Collection<Object>> en = it.next();

			System.out.println(en.getKey());
		}

	}

	@Test
	public void testGrokMy() throws Exception {

		createMorphline("grokMyTranslog.conf");
		long start = System.currentTimeMillis();

		for (int i = 0; i < 1; i++) {
			Record record = new Record();
			String msg = "Jun 10 04:42:51 INFO FLOW - [HST:NEW_WY_APP3,SID:N/A,CST:100000154474,IP:183.62.21.82 ,BIZ:creditCardManage.getCreditAccountInfo] 588231 The step [ExtractICollDataToContextAction0] returns value: 2";

			record.put(Fields.ATTACHMENT_BODY, msg.getBytes());

			Notifications.notifyStartSession(morphline);
			System.out.println((morphline.process(record)));

			System.out.println(finalChid.getRecords());
			record = finalChid.getRecords().get(0);
			ListMultimap<String, Object> list = record.getFields();

			Map<String, Collection<Object>> map = list.asMap();

			Set<Entry<String, Collection<Object>>> set = map.entrySet();

			Iterator<Entry<String, Collection<Object>>> it = set.iterator();

			while (it.hasNext()) {
				Entry<String, Collection<Object>> en = it.next();

				System.out.println(en.getKey());
			}

		}

		long end = System.currentTimeMillis();
		System.out.println((end - start) / 1000);

	}

	/**
	 * {cisco_level=[3], cisco_message_code=[%myproduct-3-mysubfacility-251010],
	 * cisco_message_id=[251010], cisco_product=[myproduct],
	 * cisco_subfacility=[mysubfacility], message=[<179>Jun 10 04:42:51 www.foo.com
	 * Jun 10 2013 04:42:51 : %myproduct-3-mysubfacility-251010: Health probe failed
	 * for server 1.2.3.4 on port 8083, connection refused by server],
	 * syslog_message=[%myproduct-3-mysubfacility-251010: Health probe failed for
	 * server 1.2.3.4 on port 8083, connection refused by server]}
	 * 
	 * @throws Exception
	 */
	@Test
	public void testGrokSyslogNgCisco() throws Exception {
		createMorphline("test-morphlines\\grokSyslogNgCisco.conf");
		Record record = new Record();
		String msg = "<179>Jun 10 04:42:51 www.foo.com Jun 10 2013 04:42:51 : %myproduct-3-mysubfacility-251010: "
				+ "Health probe failed for server 1.2.3.4 on port 8083, connection refused by server";
		record.put(Fields.ATTACHMENT_BODY, msg.getBytes());
		System.out.println((morphline.process(record)));

		morphline.process(record);
		System.out.println(finalChid.getRecords().get(0));

	}

	@Test
	public void patterTest() {
		Pattern p = Pattern.compile("(?<=\\=)(.+?)(?=\\;)");
		Matcher m = p.matcher(
				"sid=WDPT;rid=PCRM;trade=WDVC02;t0=09:06:02.367;sn=2015082409250101000001;code=00;msg=GWERR53;reqIp=21.96.60.180");
		while (m.find()) {
			System.out.println(m.group());
		}

	}

	@Test
	public void transLogTest() {

		// Notifications.notifyBeginTransaction(morphline);
		Record record = new Record();
		record.put(Fields.ATTACHMENT_BODY, "sid=WDPT;rid=PCRM;trade=WDVC02".getBytes());

		morphline.process(record);
		// Notifications.notifyCommitTransaction(morphline);

		System.out.println(finalChid.getRecords().get(0));

	}

	@Test
	public void regexTest() {

		String srcStr = "sid=WDPT;rid=PCRM;trade=WDVC02;t0=09:06:02.367;sn=2015082409250101000001;code=00;msg=GWERR53;reqIp=21.96.60.180";
		Record record = new Record();
		record.put(Fields.ATTACHMENT_BODY, srcStr.getBytes());
		morphline.process(record);
		System.out.println(toEvent(finalChid.getRecords().get(0)));

	}

	private Event toEvent(Record record) {
		Map<String, String> headers = new HashMap<String, String>();
		Map<String, Collection<Object>> recordMap = record.getFields().asMap();
		byte[] body = null;
		for (Map.Entry<String, Collection<Object>> entry : recordMap.entrySet()) {
			if (entry.getValue().size() > 1) {
				throw new FlumeException(
						getClass().getName() + " must not generate more than one output value per record field");
			}
			assert entry.getValue().size() != 0;
			Object firstValue = entry.getValue().iterator().next();
			if (Fields.ATTACHMENT_BODY.equals(entry.getKey())) {
				if (firstValue instanceof byte[]) {
					body = (byte[]) firstValue;
				} else if (firstValue instanceof InputStream) {
					try {
						body = ByteStreams.toByteArray((InputStream) firstValue);
					} catch (IOException e) {
						throw new FlumeException(e);
					}
				} else {
					throw new FlumeException(getClass().getName()
							+ " must non generate attachments that are not a byte[] or InputStream");
				}
			} else {
				headers.put(entry.getKey(), firstValue.toString());
			}
		}
		return EventBuilder.withBody(body, headers);
	}

	public void createMorphline(String path) {

		FaultTolerance faultTolerance = new FaultTolerance(false, false);

		morphlineContext = new MorphlineContext.Builder().setExceptionHandler(faultTolerance)
				.setMetricRegistry(SharedMetricRegistries.getOrCreate("testId")).build();
		finalChid = new EscafMorphlineInterceptor.Collector();
		Config override = ConfigFactory.empty();
		morphline = new Compiler().compile(
				new File("F:\\workspace\\flume\\flume-ng-escaf\\src\\test\\resources\\" + path), "morphline1",
				morphlineContext, finalChid, override);

	}

}
