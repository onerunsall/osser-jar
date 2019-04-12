package oss.launcher;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.JdbcUtils;
import com.giveup.SplitUtils;
import com.giveup.UrlUtils;
import com.giveup.ValueUtils;

public class RealizeTmpFileTask implements Runnable {

	private static Logger logger = Logger.getLogger(RealizeTmpFileTask.class);
	private Config config;

	public RealizeTmpFileTask(Config config) {
		this.config = config;
	}

	@Override
	public void run() {
		logger.info("执行任务 RealizeTmpFileTask");
		Connection connection = null;
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		PreparedStatement pst2 = null;
		String sql = null;
		String sql1 = null;
		String sql2 = null;
		try {
			connection = config.dataSource.getConnection();

			sql = "select path from t_torealize limit 0,50";
			sql1 = "delete from t_torealize where path=?";
			sql2 = "update t_file set tmpIf=0 where id=?";
			pst = connection.prepareStatement(sql);
			pst1 = connection.prepareStatement(sql1);
			pst2 = connection.prepareStatement(sql2);
			List<Map> realizes = JdbcUtils.parseResultSetOfList(JdbcUtils.runQuery(pst, sql));
			pst.close();

			connection.setAutoCommit(false);
			for (Map realize : realizes) {
				try {
					String tmpPathSrc = ValueUtils.toString(realize.get("path"));
					if (tmpPathSrc != null && !tmpPathSrc.isEmpty()) {
						String tmpPath = tmpPathSrc;
						if (Pattern.compile("^(?i)((http://)|(https://))*$").matcher(tmpPath).matches())
							tmpPath = tmpPath.replaceAll("^(?i)((http://)|(https://)).*?/", "/");

						tmpPath = UrlUtils.twistingPathSeparator(tmpPath);

						String fileId = tmpPath.replaceAll("^.*/", "").replaceAll("\\..*$", "");

						JdbcUtils.runUpdate(pst2, sql2, fileId);
						JdbcUtils.runUpdate(pst1, sql1, tmpPathSrc);

						List<String> tmpPathParts = new ArrayList<String>();
						tmpPathParts.addAll(Arrays.asList(SplitUtils.toArray(tmpPath, "/", true)));
						if (tmpPathParts.contains("tmp")) {
							if (tmpPathParts.size() > 2)
								tmpPathParts.remove("tmp");

							String realPath = UrlUtils.buildPath(tmpPathParts.toArray(new String[] {}));
							File tmpFile = new File(config.webroot, tmpPath);
							File realizeFile = null;
							if (tmpFile.exists()) {
								realizeFile = new File(config.webroot, realPath);
								if (!realizeFile.getParentFile().exists())
									realizeFile.getParentFile().mkdirs();

								tmpFile.renameTo(realizeFile);
							}
						}
					} else {
						JdbcUtils.runUpdate(pst1, sql1, tmpPathSrc);
					}
					connection.commit();
				} catch (Exception e) {
					logger.info(ExceptionUtils.getStackTrace(e));
					connection.rollback();
				} finally {

				}
			}
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
		} finally {
			if (pst != null)
				try {
					pst.close();
				} catch (SQLException e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
			if (pst1 != null)
				try {
					pst1.close();
				} catch (SQLException e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
		}
	}

	public static void main(String[] args) throws MalformedURLException {
		String s = "http://121.40.168.181:81/zhu/j/ianlin1990.d";
		java.net.URL pathurl = new java.net.URL(s);
		String host = pathurl.getHost();
		Integer port = pathurl.getPort();
		System.out.println(port);
		System.out.println(s.substring(s.indexOf(host) + host.length()));
	}

}
