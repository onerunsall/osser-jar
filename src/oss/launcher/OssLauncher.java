package oss.launcher;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.JdbcUtils;
import com.giveup.StrUtils;

public class OssLauncher implements Runnable {
	private static DataSource dataSource = null;
	private static String environment;

	private static Logger logger = Logger.getLogger(OssLauncher.class);

	// 创建队列 可接受SmsPayload类的任务 该队列是阻塞队列，也就是当没有任务的时候队列阻塞（也就是暂停）
	private static BlockingQueue<Payload> queue = new LinkedBlockingDeque<Payload>();

	public static void deleteUnused(Connection connection, String oldUrl, String newUrl) throws InterruptedException {
		deleteUnused(connection, 0, oldUrl, newUrl);
	}

	public static void deleteUnused(String oldUrl, String newUrl) throws InterruptedException {
		deleteUnused(0, oldUrl, newUrl);
	}

	public static void deleteUnused(Connection connection, int delay, String oldUrl, String newUrl)
			throws InterruptedException {
		if (newUrl != null && !newUrl.trim().isEmpty() && oldUrl != null && !oldUrl.trim().isEmpty()
				&& !oldUrl.equals(newUrl))
			delete(connection, delay, oldUrl);
	}

	public static void deleteUnused(int delay, String oldUrl, String newUrl) throws InterruptedException {
		if (newUrl != null && !newUrl.trim().isEmpty() && oldUrl != null && !oldUrl.trim().isEmpty()
				&& !oldUrl.equals(newUrl))
			delete(delay, oldUrl);
	}

	public static void delete(Connection connection, String url) throws InterruptedException {
		delete(connection, 0, url);
	}

	public static void delete(String url) throws InterruptedException {
		delete(0, url);
	}

	public static void delete(Connection connection, int delay, String url) throws InterruptedException {
		delete(connection, delay, new String[] { url });
	}

	public static void delete(int delay, String url) throws InterruptedException {
		delete(delay, new String[] { url });
	}

	public static void delete(List<String> urls) throws InterruptedException {
		delete(0, urls);
	}

	public static void delete(Connection connection, List<String> urls) throws InterruptedException {
		delete(connection, 0, urls);
	}

	public static void delete(Connection connection, int delay, List<String> urls) throws InterruptedException {
		delete(connection, delay, urls.toArray(new String[] {}));
	}

	public static void delete(int delay, List<String> urls) throws InterruptedException {
		delete(delay, urls.toArray(new String[] {}));
	}

	public static void delete(Connection connection, int delay, String... urls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", urls);
		data.put("delay", delay);
		Payload payload = new Payload();
		payload.type = "delete";
		payload.data = data;
		doDelete(payload, connection);
	}

	public static void delete(int delay, String... urls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", urls);
		data.put("delay", delay);
		Payload payload = new Payload();
		payload.type = "delete";
		payload.data = data;
		queue.put(payload);
	}

	public static void realize(Connection connection, String tmpUrl) throws InterruptedException {
		realize(connection, new String[] { tmpUrl });
	}

	public static void realize(String tmpUrl) throws InterruptedException {
		realize(new String[] { tmpUrl });
	}

	public static void realize(Connection connection, List<String> tmpUrls) throws InterruptedException {
		realize(connection, tmpUrls.toArray(new String[] {}));
	}

	public static void realize(List<String> tmpUrls) throws InterruptedException {
		realize(tmpUrls.toArray(new String[] {}));
	}

	public static void realize(String... tmpUrls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", tmpUrls);
		Payload payload = new Payload();
		payload.type = "realize";
		payload.data = data;
		queue.put(payload);
	}

	public static void realize(Connection connection, String... tmpUrls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", tmpUrls);
		Payload payload = new Payload();
		payload.type = "realize";
		payload.data = data;
		doRealize(payload, connection);
	}

	private static class Payload {
		String type;
		Map data;
	}

	public OssLauncher(DataSource dataSource, String environment) {
		OssLauncher.dataSource = dataSource;
		OssLauncher.environment = environment;
		if (!"prod".equals(environment) && !"test".equals(environment) && !"dev".equals(environment))
			throw new RuntimeException("environment有误");
	}

	private static void doDelete(Payload payload, Connection connection) throws InterruptedException {
		PreparedStatement pst = null;
		List sqlParams = null;
		try {
			int delay = (Integer) payload.data.get("delay");
			String[] urls = (String[]) payload.data.get("urls");
			String sql = new StringBuilder(
					"insert into oss_" + OssLauncher.environment + ".t_todel (path,delay) values(?,?)").toString();
			List sqlBatchParams = new ArrayList();
			for (String url : urls) {
				if (url != null && !url.isEmpty()) {
					sqlParams = new ArrayList();
					sqlParams.add(url);
					sqlParams.add(delay);
					sqlBatchParams.add(sqlParams);
				}
			}
			pst = connection.prepareStatement(sql);
			JdbcUtils.runBatch(pst, sql, sqlBatchParams);
			pst.close();
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
		} finally {
			if (pst != null)
				try {
					pst.close();
				} catch (SQLException e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
		}
	}

	private static void doRealize(Payload payload, Connection connection) throws InterruptedException {
		PreparedStatement pst = null;
		List sqlParams = null;
		try {
			String[] urls = (String[]) payload.data.get("urls");
			String sql = new StringBuilder(
					"insert into oss_" + OssLauncher.environment + ".t_torealize (path) values(?)").toString();
			pst = connection.prepareStatement(sql);
			List sqlBatchParams = new ArrayList();
			for (String url : urls) {
				if (url != null && !url.isEmpty()) {
					sqlParams = new ArrayList();
					sqlParams.add(url);
					sqlBatchParams.add(sqlParams);
				}
			}
			JdbcUtils.runBatch(pst, sql, sqlBatchParams);
			pst.close();
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
		} finally {
			if (pst != null)
				try {
					pst.close();
				} catch (SQLException e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
		}
	}

	@Override
	public void run() {
		Connection connection = null;
		try {
			while (true) {
				try {
					Payload payload = queue.take();
					connection = dataSource.getConnection();
					logger.debug("发现新的oss异步任务 ： " + payload);
					if (payload.type.equals("delete"))
						doDelete(payload, connection);
					if (payload.type.equals("realize"))
						doRealize(payload, connection);
				} catch (Exception e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				} finally {
					if (connection != null)
						try {
							connection.close();
						} catch (SQLException e) {
							logger.info(ExceptionUtils.getStackTrace(e));
						}
				}
			}
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
		}

	}

}
