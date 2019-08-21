package com.giveup.osser;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.JdbcUtils;

class DeleteNotrecordFileTask implements Runnable {

	private static Logger logger = Logger.getLogger(DeleteNotrecordFileTask.class);

	private Config config;

	public DeleteNotrecordFileTask(Config config) {
		this.config = config;
	}

	@Override
	public void run() {
		logger.info("执行任务 DeleteNorecordFileTask");
		Connection connection = null;
		try {
			// 查询待删除的文件
			StringBuilder sql = new StringBuilder("select id from t_file where id=?");

			connection = config.dataSource.getConnection();

			File projectOssRoot = new File(config.webroot, "oss/" + config.project);
			if (projectOssRoot.exists() && projectOssRoot.isDirectory()) {
				projectOssRoot.listFiles(new FileFilter() {

					FileFilter setConnection(Connection connection) throws SQLException {
						this.connection = connection;
						this.pst = this.connection.prepareStatement(sql.toString());
						return this;
					}

					Connection connection = null;
					PreparedStatement pst = null;

					@Override
					public boolean accept(File file) {
						if (file.isDirectory())
							return false;
						try {
							Map row = JdbcUtils.parseResultSetOfOne(
									JdbcUtils.runQuery(pst, sql.toString(), file.getName().replaceAll("\\..*$", "")));
							if (row == null) {
								if (!file.delete()) {
									logger.info("删除文件" + file.getAbsolutePath());
									String command = new StringBuilder("rm -rf ")
											.append(file.getAbsolutePath().toString()).toString();
									logger.info(command);
									Process ps = Runtime.getRuntime().exec(command);
									ps.waitFor();
									ps.destroy();
								}
							}
						} catch (Exception e) {
							logger.info(ExceptionUtils.getStackTrace(e));
						}
						return false;
					}
				}.setConnection(connection));
			}
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

	public static void main(String[] args) throws MalformedURLException {
		String s = "http://121.40.168.181:81/zhu/j/ianlin1990.d";
		java.net.URL pathurl = new java.net.URL(s);
		String host = pathurl.getHost();
		Integer port = pathurl.getPort();
		System.out.println(port);
		System.out.println("q.qw.e".replaceAll("\\..*?&", ""));
	}

}
