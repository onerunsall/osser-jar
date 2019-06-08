package oss.launcher;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
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
		StringBuilder sql = null;
		StringBuilder sql1 = null;
		StringBuilder sql2 = null;
		StringBuilder sql3 = null;
		StringBuilder sql4 = null;
		List sqlParams = null;
		List sqlParams1 = null;
		List sqlParams2 = null;
		List sqlParams3 = null;
		List sqlParams4 = null;
		try {
			// 查询待删除的文件
			sql = new StringBuilder(
					"select url from t_file_del where addTime=null or delay=0 or (addTime is not null and delay>0 and timestampdiff(SECOND,addTime,now())>delay) order by delay asc,addTime desc limit 0,50");
			sql1 = new StringBuilder("select id fildId,cover,path,tmpIf from t_file where id=? ");
			sql3 = new StringBuilder("delete from t_file_del where url=?");
			sql2 = new StringBuilder("delete from t_file where id=?");
			sql4 = new StringBuilder("insert into t_file_del (url) values(?)");

			connection = config.dataSource.getConnection();
			connection.setAutoCommit(false);

			pst = connection.prepareStatement(sql.toString());
			List<Object> rows = JdbcUtils.parseResultSetOfThinList(JdbcUtils.runQuery(pst, sql.toString()));
			pst.close();
			List<String> urls = Arrays.asList(rows.toArray(new String[] {}));

			// 将超时的临时文件转入待删除
			JdbcUtils.runUpdate(connection,
					"insert into t_file_del(url) select a.path  from t_file a where a.tmpIf=1 and now() >SUBDATE(a.addTime,interval -30 minute)");

			connection.commit();

			if (!urls.isEmpty()) {
				pst1 = connection.prepareStatement(sql1.toString());
				pst2 = connection.prepareStatement(sql2.toString());
				pst3 = connection.prepareStatement(sql3.toString());
				pst4 = connection.prepareStatement(sql4.toString());
			}
			for (int i = 0; i < urls.size(); i++) {
				String url = StringUtils.trimToNull(urls.get(i));
				if (url == null)
					continue;
				try {
					String path = url.replaceAll("\\\\", "/").replaceFirst("(?i)((http:/+)|(https:/+))", "")
							.replaceFirst(".*?/", "/");
					String fileName = path.replaceAll("^.*/", "").replaceAll("\\..*$", "");
					String fileId = fileName;
					String fileExtName = path.replaceAll("^.*\\.", "");
					File file = new File(config.webroot, path);
					logger.debug("delete " + file.getAbsoluteFile());
					// 查找file记录
					Map fileRow = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst1, sql1.toString(), fileId));

					// 删除del记录
					JdbcUtils.runUpdate(pst3, sql3.toString(), url);

					if (fileRow != null) {
						if (fileRow.get("cover") != null) {
							// 记录del封面
							JdbcUtils.runUpdate(pst4, sql4.toString(), fileRow.get("cover"));
						}
						// 删除file记录
						JdbcUtils.runUpdate(pst2, sql2.toString(), fileId);
					}

					logger.debug("delete real");
					if (!path.startsWith("/oss/" + config.project))
						logger.debug("只能删除本项目下的文件");
					else {
						logger.debug("file.exists " + file.exists());
						// 删除改目标文件的变种
						File fileVariantFolder = new File(file.getParent(), UrlUtils.fileExtStrip(file.getName()));
						if (fileVariantFolder.exists()
								&& !fileVariantFolder.getAbsolutePath().equals(file.getParentFile().getAbsolutePath()))
							if (!IOUtils.deleteRecursion(fileVariantFolder))
								throw new RuntimeException("删除变种文件夹失败" + fileVariantFolder.getAbsolutePath());
						if (!file.delete())
							throw new RuntimeException("删除文件失败" + file.getAbsolutePath());
					}
					connection.commit();

				} catch (Exception e) {
					logger.debug(ExceptionUtils.getStackTrace(e));
					connection.rollback();
				} finally {

				}
			}

			JdbcUtils.runUpdate(connection,
					"INSERT INTO t_file_del (url) SELECT path FROM (SELECT path,(SELECT COUNT(1) FROM t_file WHERE linkedFileId=t.id) linkCount FROM t_file t WHERE linkedFileId IS NULL ) tt WHERE tt.linkCount =0");
			JdbcUtils.runUpdate(connection,
					"DELETE FROM t_file WHERE id IN (SELECT id FROM (SELECT id,(SELECT COUNT(1) FROM t_file WHERE linkedFileId=t.id) linkCount FROM t_file t WHERE linkedFileId IS NULL ) tt WHERE tt.linkCount =0)");
			connection.commit();
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
		} finally {
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					logger.info(ExceptionUtils.getStackTrace(e));
				}
			if (pst != null)
				try {
					pst.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (pst1 != null)
				try {
					pst1.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (pst2 != null)
				try {
					pst2.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (pst3 != null)
				try {
					pst3.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (pst4 != null)
				try {
					pst4.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
