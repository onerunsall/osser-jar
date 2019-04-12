package oss.launcher;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.IOUtils;
import com.giveup.JdbcUtils;
import com.giveup.UrlUtils;
import com.giveup.ValueUtils;

import oss.launcher.OssLauncherTask.Payload;

public class OssLauncher {

	private static Logger logger = Logger.getLogger(OssLauncher.class);
	private Config config;

	public void deleteUnused(Connection connection, String oldUrl, String newUrl) throws InterruptedException {
		deleteUnused(connection, 0, oldUrl, newUrl);
	}

	public void deleteUnused(String oldUrl, String newUrl) throws InterruptedException {
		deleteUnused(0, oldUrl, newUrl);
	}

	public void deleteUnused(Connection connection, int delay, String oldUrl, String newUrl)
			throws InterruptedException {
		if (newUrl != null && !newUrl.trim().isEmpty() && oldUrl != null && !oldUrl.trim().isEmpty()
				&& !oldUrl.equals(newUrl))
			delete(connection, delay, oldUrl);
	}

	public void deleteUnused(int delay, String oldUrl, String newUrl) throws InterruptedException {
		if (newUrl != null && !newUrl.trim().isEmpty() && oldUrl != null && !oldUrl.trim().isEmpty()
				&& !oldUrl.equals(newUrl))
			delete(delay, oldUrl);
	}

	public void delete(Connection connection, String url) throws InterruptedException {
		delete(connection, 0, url);
	}

	public void delete(String url) throws InterruptedException {
		delete(0, url);
	}

	public void delete(Connection connection, int delay, String url) throws InterruptedException {
		delete(connection, delay, new String[] { url });
	}

	public void delete(int delay, String url) throws InterruptedException {
		delete(delay, new String[] { url });
	}

	public void delete(List<String> urls) throws InterruptedException {
		delete(0, urls);
	}

	public void delete(Connection connection, List<String> urls) throws InterruptedException {
		delete(connection, 0, urls);
	}

	public void delete(Connection connection, int delay, List<String> urls) throws InterruptedException {
		delete(connection, delay, urls.toArray(new String[] {}));
	}

	public void delete(int delay, List<String> urls) throws InterruptedException {
		delete(delay, urls.toArray(new String[] {}));
	}

	public void delete(Connection connection, int delay, String... urls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", urls);
		data.put("delay", delay);
		Payload payload = new Payload();
		payload.type = "delete";
		payload.data = data;
		config.ossLauncherTask.doDelete(payload, connection);
	}

	public void delete(int delay, String... urls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", urls);
		data.put("delay", delay);
		Payload payload = new Payload();
		payload.type = "delete";
		payload.data = data;
		config.ossLauncherTask.queue.put(payload);
	}

	public void realize(Connection connection, String tmpUrl) throws InterruptedException {
		realize(connection, new String[] { tmpUrl });
	}

	public void realize(String tmpUrl) throws InterruptedException {
		realize(new String[] { tmpUrl });
	}

	public void realize(Connection connection, List<String> tmpUrls) throws InterruptedException {
		realize(connection, tmpUrls.toArray(new String[] {}));
	}

	public void realize(List<String> tmpUrls) throws InterruptedException {
		realize(tmpUrls.toArray(new String[] {}));
	}

	public void realize(String... tmpUrls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", tmpUrls);
		Payload payload = new Payload();
		payload.type = "realize";
		payload.data = data;
		config.ossLauncherTask.queue.put(payload);
	}

	public void realize(Connection connection, String... tmpUrls) throws InterruptedException {
		Map data = new HashMap();
		data.put("urls", tmpUrls);
		Payload payload = new Payload();
		payload.type = "realize";
		payload.data = data;
		config.ossLauncherTask.doRealize(payload, connection);
	}

	public OssLauncher(Config config) {
		if (!"prod".equals(config.environment) && !"test".equals(config.environment)
				&& !"dev".equals(config.environment))
			throw new RuntimeException("environment有误");
		this.config = config;
	}

	public String save(InputStream is, String originalFileName, String cover, String duration) throws Exception {
		Connection connection = null;
		PreparedStatement pst = null;
		StringBuilder sql = null;
		StringBuilder sql1 = null;
		StringBuilder sql2 = null;
		List sqlParams = null;
		try {

			File projectFolder = new File(config.webroot + "/oss", config.project);
			if (!projectFolder.exists())
				throw new RuntimeException("project not exist.");

			File tmpFolder = new File(projectFolder, "tmp");
			if (!tmpFolder.exists())
				tmpFolder.mkdirs();
			Date now = new Date();
			// 保存文件
			String url = null;
			connection = config.dataSource.getConnection();
			connection.setAutoCommit(false);
			sql = new StringBuilder(
					"insert into t_file (id,md5,project,name,size,duration,cover,path,linkedFileId,tmpIf) values(?,?,?,?,?,?,?,?,?,?)");
			sql1 = new StringBuilder("select id fileId,path,duration from t_file where project=? and md5=? limit 1");

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
			String ext = UrlUtils.fileExtGet(originalFileName);
			String tmpFileId = sdf.format(now) + RandomStringUtils.randomNumeric(6);
			String realFileId = tmpFileId;
			String linkFileId = sdf.format(now) + RandomStringUtils.randomNumeric(6);
			String tmpFileName = StringUtils.isEmpty(ext) ? tmpFileId : (tmpFileId + "." + ext);
			String realFileName = tmpFileName;
			String linkFileName = StringUtils.isEmpty(ext) ? linkFileId : (linkFileId + "." + ext);
			String md5 = null;
			String tmpFilePath = UrlUtils.buildPath("/", "oss", config.project, "tmp", tmpFileName);
			String realFilePath = UrlUtils.buildPath("/", "oss", config.project, realFileName);
			String linkFilePath = UrlUtils.buildPath("/", "oss", config.project, linkFileName);
			url = linkFilePath;
			File tmpFile = new File(config.webroot, tmpFilePath);
			File realFile = new File(config.webroot, realFilePath);
			File linkFile = new File(config.webroot, linkFilePath);

			// 保存上传的文件到临时目录
			IOUtils.write(is, tmpFile);
			md5 = IOUtils.getMD5(tmpFile);

			sqlParams = new ArrayList();
			sqlParams.add(config.project);
			sqlParams.add(md5);
			pst = connection.prepareStatement(sql1.toString());
			Map existFileRow = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst, sql1.toString(), sqlParams));
			pst.close();

			realFileId = existFileRow == null ? realFileId : ValueUtils.toString(existFileRow.get("fileId"));
			sqlParams = new ArrayList();
			sqlParams.add(linkFileId);
			sqlParams.add(null);
			sqlParams.add(config.project);
			sqlParams.add(linkFileName);
			sqlParams.add(tmpFile.length());
			sqlParams.add(duration);
			sqlParams.add(cover);
			sqlParams.add(linkFilePath);
			sqlParams.add(realFileId);
			sqlParams.add(1);
			pst = connection.prepareStatement(sql.toString());
			JdbcUtils.runUpdate(pst, sql.toString(), sqlParams);
			pst.close();

			if (existFileRow == null) {
				sqlParams = new ArrayList();
				sqlParams.add(realFileId);
				sqlParams.add(md5);
				sqlParams.add(config.project);
				sqlParams.add(realFileName);
				sqlParams.add(tmpFile.length());
				sqlParams.add(duration);
				sqlParams.add(null);
				sqlParams.add(realFilePath);
				sqlParams.add(null);
				sqlParams.add(0);
				pst = connection.prepareStatement(sql.toString());
				JdbcUtils.runUpdate(pst, sql.toString(), sqlParams);
				pst.close();

//				OsCommandUtils.exec(new StringBuilder("").append(" ln -s ")
//						.append(realFile.getAbsolutePath()).append(" ").append(linkFile.getAbsolutePath()).toString(),
//						new StringBuilder("cmd.exe /c ").append(" mklink ")
//						.append(linkFile.getAbsolutePath()).append(" ").append(realFile.getAbsolutePath())
//						.toString());
				String command = new StringBuilder("whoami").toString();
				logger.debug(command);
				Process ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				logger.debug(org.apache.commons.io.IOUtils.toString(ps.getInputStream()));
				ps.destroy();

				command = new StringBuilder("/bin/sh -c cd ").append(realFile.getParent()).toString();
				logger.debug(command);
				ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				ps.destroy();

				command = new StringBuilder("pwd").toString();
				logger.debug(command);
				ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				logger.debug(org.apache.commons.io.IOUtils.toString(ps.getInputStream()));
				ps.destroy();

				command = new StringBuilder(" ln -s ").append("." + File.separator + realFile.getName()).append(" ")
						.append(linkFile.getAbsolutePath()).toString();
				logger.debug(command);
				ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				ps.destroy();

				tmpFile.renameTo(realFile);
			} else {
				realFile = new File(config.webroot, ValueUtils.toString(existFileRow.get("path")));

				String command = new StringBuilder("whoami").toString();
				logger.debug(command);
				Process ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				logger.debug(org.apache.commons.io.IOUtils.toString(ps.getInputStream()));
				ps.destroy();

				command = new StringBuilder("/bin/sh -c cd ").append(realFile.getParent()).toString();
				logger.debug(command);
				ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				ps.destroy();

				command = new StringBuilder("pwd").toString();
				logger.debug(command);
				ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				logger.debug(org.apache.commons.io.IOUtils.toString(ps.getInputStream()));
				ps.destroy();

				command = new StringBuilder(" ln -s ").append("." + File.separator + realFile.getName()).append(" ")
						.append(linkFile.getAbsolutePath()).toString();
				logger.debug(command);
				ps = Runtime.getRuntime().exec(command);
				ps.waitFor();
				ps.destroy();

//				OsCommandUtils.exec(
//						new StringBuilder("").append(" ln -s ").append(realFile.getAbsolutePath()).append(" ")
//								.append(linkFile.getAbsolutePath()).toString(),
//						new StringBuilder("cmd.exe /c ").append(" mklink ").append(linkFile.getAbsolutePath())
//								.append(" ").append(realFile.getAbsolutePath()).toString());
				tmpFile.delete();
			}

			connection.commit();

			return url;
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
			if (connection != null)
				connection.rollback();
			throw e;
		} finally {
			if (connection != null)
				connection.close();
		}
	}
}
