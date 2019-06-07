//package oss.launcher;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingDeque;
//
//import org.apache.commons.lang3.exception.ExceptionUtils;
//import org.apache.log4j.Logger;
//
//import com.giveup.JdbcUtils;
//
//class OssLauncherTask implements Runnable {
//
//	private static Logger logger = Logger.getLogger(OssLauncherTask.class);
//	private Config config;
//
//	// 创建队列 可接受SmsPayload类的任务 该队列是阻塞队列，也就是当没有任务的时候队列阻塞（也就是暂停）
//	BlockingQueue<Payload> queue = new LinkedBlockingDeque<Payload>();
//
//	static class Payload {
//		String type;
//		Map data;
//	}
//
//	public OssLauncherTask(Config config) {
//		this.config = config;
//	}
//
//	void doDelete(Payload payload, Connection connection) throws InterruptedException {
//		PreparedStatement pst = null;
//		List sqlParams = null;
//		try {
//			int delay = (Integer) payload.data.get("delay");
//			String[] urls = (String[]) payload.data.get("urls");
//			String sql = new StringBuilder(
//					"insert into oss_" + config.environment + ".t_todel (path,delay) values(?,?)").toString();
//			List sqlBatchParams = new ArrayList();
//			for (String url : urls) {
//				if (url != null && !url.isEmpty()) {
//					sqlParams = new ArrayList();
//					sqlParams.add(url);
//					sqlParams.add(delay);
//					sqlBatchParams.add(sqlParams);
//				}
//			}
//			pst = connection.prepareStatement(sql);
//			JdbcUtils.runBatch(pst, sql, sqlBatchParams);
//			pst.close();
//		} catch (Exception e) {
//			logger.info(ExceptionUtils.getStackTrace(e));
//		} finally {
//			if (pst != null)
//				try {
//					pst.close();
//				} catch (SQLException e) {
//					logger.info(ExceptionUtils.getStackTrace(e));
//				}
//		}
//	}
//
//	void doRealize(Payload payload, Connection connection) throws InterruptedException {
//		PreparedStatement pst = null;
//		List sqlParams = null;
//		try {
//			String[] urls = (String[]) payload.data.get("urls");
//			String sql = new StringBuilder("insert into oss_" + config.environment + ".t_torealize (path) values(?)")
//					.toString();
//			pst = connection.prepareStatement(sql);
//			List sqlBatchParams = new ArrayList();
//			for (String url : urls) {
//				if (url != null && !url.isEmpty()) {
//					sqlParams = new ArrayList();
//					sqlParams.add(url);
//					sqlBatchParams.add(sqlParams);
//				}
//			}
//			JdbcUtils.runBatch(pst, sql, sqlBatchParams);
//			pst.close();
//		} catch (Exception e) {
//			logger.info(ExceptionUtils.getStackTrace(e));
//		} finally {
//			if (pst != null)
//				try {
//					pst.close();
//				} catch (SQLException e) {
//					logger.info(ExceptionUtils.getStackTrace(e));
//				}
//		}
//	}
//
//	@Override
//	public void run() {
//		Connection connection = null;
//		try {
//			while (true) {
//				try {
//					Payload payload = queue.take();
//					connection = config.dataSource.getConnection();
//					logger.debug("发现新的oss异步任务 ： " + payload);
//					if (payload.type.equals("delete"))
//						doDelete(payload, connection);
//					if (payload.type.equals("realize"))
//						doRealize(payload, connection);
//				} catch (Exception e) {
//					logger.info(ExceptionUtils.getStackTrace(e));
//				} finally {
//					if (connection != null)
//						try {
//							connection.close();
//						} catch (SQLException e) {
//							logger.info(ExceptionUtils.getStackTrace(e));
//						}
//				}
//			}
//		} catch (Exception e) {
//			logger.info(ExceptionUtils.getStackTrace(e));
//		}
//
//	}
//
//}
