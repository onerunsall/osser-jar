package oss.launcher;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.JdbcUtils;
import com.giveup.ValueUtils;

class DeleteNotexistFileTask implements Runnable {

	private static Logger logger = Logger.getLogger(DeleteNotexistFileTask.class);
	private static int pageNo = 0;
	int pageSize = 50;
	private Config config;

	public DeleteNotexistFileTask(Config config) {
		this.config = config;
	}

	@Override
	public void run() {
		logger.info("执行任务 DeleteNotexistFileTask");
		Connection connection = null;
		try {
			// 查询待删除的文件

			connection = config.dataSource.getConnection();
			connection.setAutoCommit(false);

			Integer notCheckCount = JdbcUtils.runQueryOneInteger(connection,
					"select count(1) from t_file where checkIs=0 ");
			if (notCheckCount == 0) {
				JdbcUtils.runUpdate(connection, "update t_file set checkIs=0 ");
				pageNo = 0;
			}
			connection.commit();

			pageNo++;
			List<Map> rows = JdbcUtils.runQueryList(connection,
					"select id fileId,path from t_file where checkIs=0 order by addTime asc,id asc limit ?,?",
					pageSize * (pageNo - 1), pageSize);
			for (int i = 0; i < rows.size(); i++) {
				try {
					Map row = rows.get(i);
					String fileId = ValueUtils.toString(row.get("fileId"));
					String path = ValueUtils.toString(row.get("path"));
					File file = new File(config.webroot, path);
					if (!file.exists()) {
						JdbcUtils.runUpdate(connection, "delete from t_file where id=? ", fileId);
						if (!file.delete()) {
							logger.info("删除文件" + file.getAbsolutePath());
							String command = new StringBuilder("rm -rf ").append(file.getAbsolutePath().toString())
									.toString();
							logger.info(command);
							Process ps = Runtime.getRuntime().exec(command);
							ps.waitFor();
							ps.destroy();
						}
					} else {
						JdbcUtils.runUpdate(connection, "update t_file set checkIs=1  where id=? ", fileId);
					}
					connection.commit();
				} catch (Exception e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
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
