package oss.launcher;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.IOUtils;
import com.giveup.JdbcUtils;
import com.giveup.UrlUtils;
import com.giveup.ValueUtils;

class DeleteFileTask implements Runnable {

	private static Logger logger = Logger.getLogger(DeleteFileTask.class);

	private Config config;

	public DeleteFileTask(Config config) {
		this.config = config;
	}

	@Override
	public void run() {
		logger.info("执行任务 DeleteFileTask");
		Connection connection = null;
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		PreparedStatement pst2 = null;
		PreparedStatement pst3 = null;
		PreparedStatement pst4 = null;
		PreparedStatement pst5 = null;
		StringBuilder sql = null;
		StringBuilder sql1 = null;
		StringBuilder sql2 = null;
		StringBuilder sql3 = null;
		StringBuilder sql4 = null;
		StringBuilder sql5 = null;
		List sqlParams = null;
		List sqlParams1 = null;
		List sqlParams2 = null;
		List sqlParams3 = null;
		List sqlParams4 = null;
		try {
			// 查询待删除的文件
			sql = new StringBuilder("select path,addTime,delay from oss_" + config.environment
					+ ".t_todel where addTime=null or delay=0 or (addTime is not null and delay>0 and timestampdiff(SECOND,addTime,now())>delay) order by delay asc,addTime desc limit 0,50");
			sql1 = new StringBuilder(
					"select id fildId,cover,path,tmpIf from oss_" + config.environment + ".t_file where id=? ");
			sql3 = new StringBuilder("delete from oss_" + config.environment + ".t_todel where path=?");
			sql2 = new StringBuilder("delete from oss_" + config.environment + ".t_file where id=?");
			sql4 = new StringBuilder("insert into oss_" + config.environment + ".t_todel (path) values(?)");
			// 将超时的临时文件转入待删除
			sql5 = new StringBuilder("insert into oss_" + config.environment + ".t_todel(path) select a.path  from oss_"
					+ config.environment + ".t_file a where a.tmpIf=1 and now() >SUBDATE(a.addTime,interval -3 day)");
			connection = config.dataSource.getConnection();

			pst = connection.prepareStatement(sql.toString());
			List<Map> rows = JdbcUtils.parseResultSetOfList(JdbcUtils.runQuery(pst, sql.toString()));
			pst.close();

			pst5 = connection.prepareStatement(sql5.toString());
			JdbcUtils.runUpdate(pst5, sql5.toString());
			pst5.close();

			connection.setAutoCommit(false);
			for (Map row : rows) {
				try {
					String pathSrc = ValueUtils.toString(row.get("path"));
					if (pathSrc != null && !pathSrc.isEmpty()) {
						String path = pathSrc;
						if (Pattern.compile("^(?i)((http://)|(https://)).*$").matcher(path).matches())
							path = path.replaceAll("^(?i)((http://)|(https://)).*?(/|\\\\)", "/");
						path = UrlUtils.twistingPathSeparator(path);

						String fileId = path.replaceAll("^.*/", "").replaceAll("\\..*$", "");
						File file = new File(config.webroot, path);

						// // 临时文件如果存在则删除
						// List<String> realParts = new
						// ArrayList(Arrays.asList(url.split("/")));
						// if (realParts.size() >= 3) {
						// realParts.add(3, "tmp");
						// String tmpPath = UrlUtils.buildPath(realParts);
						// File tmpFile = new File(SysConstant.projectsroot,
						// tmpPath);
						// if (tmpFile.exists())
						// tmpFile.delete();
						// }

						// 查找file记录
						pst1 = connection.prepareStatement(sql1.toString());
						Map fileRow = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst1, sql1.toString(), fileId));
						pst1.close();

						// 删除del记录
						pst3 = connection.prepareStatement(sql3.toString());
						JdbcUtils.runUpdate(pst3, sql3.toString(), pathSrc);
						pst3.close();

						if (fileRow != null) {
							if (fileRow.get("cover") != null) {
								// 记录del封面
								pst4 = connection.prepareStatement(sql4.toString());
								JdbcUtils.runUpdate(pst4, sql4.toString(), fileRow.get("cover"));
								pst4.close();
							}
							// 删除file记录
							pst2 = connection.prepareStatement(sql2.toString());
							JdbcUtils.runUpdate(pst2, sql2.toString(), fileId);
							pst2.close();
						}

						// 删除目标文件
						if (file.exists()) {
							// 删除改目标文件的变种
							File fileVariantFolder = new File(file.getParent(), UrlUtils.fileExtStrip(file.getName()));
							if (fileVariantFolder.exists() && !fileVariantFolder.getAbsolutePath()
									.equals(file.getParentFile().getAbsolutePath()))
								if (!IOUtils.deleteRecursion(fileVariantFolder))
									throw new RuntimeException("删除变种文件夹失败" + fileVariantFolder.getAbsolutePath());

							if (!file.delete())
								throw new RuntimeException("删除文件失败" + file.getAbsolutePath());
						}
					} else {
						pst3 = connection.prepareStatement(sql3.toString());
						JdbcUtils.runUpdate(pst3, sql3.toString(), pathSrc);
						pst3.close();
					}
					connection.commit();
				} catch (Exception e) {
					logger.info(ExceptionUtils.getStackTrace(e));
					connection.rollback();
				} finally {
					if (pst != null)
						pst.close();
					if (pst1 != null)
						pst1.close();
					if (pst2 != null)
						pst2.close();
					if (pst3 != null)
						pst3.close();
					if (pst4 != null)
						pst4.close();
					if (pst5 != null)
						pst5.close();
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
		System.out.println(s.substring(s.indexOf(host) + host.length()));
	}

}
