package oss.launcher;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

import net.coobird.thumbnailator.Thumbnails;
import net.coobird.thumbnailator.Thumbnails.Builder;

public class OssLauncher {

	private static Logger logger = Logger.getLogger(OssLauncher.class);
	private Config config;

	public void change(Connection connection, String oldUrl, String newUrl) throws Exception {
		if (newUrl != null && !newUrl.equals(oldUrl)) {
			delete(connection, oldUrl);
		}
		realize(connection, newUrl);
	}

	public void delete(Connection connection, String... urls) throws Exception {
		StringBuilder sql = new StringBuilder("insert into t_file_del (url) values(?)");
		PreparedStatement pst = connection.prepareStatement(sql.toString());
		int n = 0;
		for (int i = 0; i < urls.length; i++) {
			String url = urls[i];
			if (url == null)
				continue;
			pst.setObject(1, url);
			pst.addBatch();
			n++;
		}
		if (n > 0)
			pst.executeBatch();
		pst.close();
	}

	public void delete(Connection connection, List<String> urls) throws Exception {
		delete(connection, urls.toArray(new String[] {}));
	}

	public void realize(Connection connection, List<String> urls) throws Exception {
		realize(connection, urls.toArray(new String[] {}));
	}

	public void realize(Connection connection, String... urls) throws Exception {
		StringBuilder sql = new StringBuilder("update t_file set tmpIf=0 where id=?");
		PreparedStatement pst = connection.prepareStatement(sql.toString());

		int n = 0;
		for (int i = 0; i < urls.length; i++) {
			String url = urls[i];
			if (url == null)
				continue;
			String path = url.replaceAll("\\\\", "/").replaceFirst("(?i)((http:/+)|(https:/+))", "")
					.replaceFirst(".*?/", "/");
			String fileName = path.replaceAll("^.*/", "").replaceAll("\\..*$", "");
			String fileId = fileName;

			pst.setObject(1, fileId);
			pst.addBatch();
			n++;
			String coverUrl = ValueUtils
					.toString(JdbcUtils.runQueryOneColumn(connection, "select cover from t_file where id=? ", fileId));
			if (coverUrl == null)
				continue;
			String coverPath = coverUrl.replaceAll("\\\\", "/").replaceFirst("(?i)((http:/+)|(https:/+))", "")
					.replaceFirst(".*?/", "/");
			String coverFileName = coverPath.replaceAll("^.*/", "").replaceAll("\\..*$", "");
			String coverFileId = coverFileName;

			pst.setObject(1, coverFileId);
			pst.addBatch();
			n++;
		}
		if (n > 0)
			pst.executeBatch();
		pst.close();
	}

	public OssLauncher(Config config) {
		this.config = config;
	}

	public static void main(String[] args) {
		String url = "http://www.baidu.com/sf/11";
		System.out.println(
				url.replaceAll("\\\\", "/").replaceFirst("(?i)((http://)|(https://))", "").replaceFirst(".*?/", "/"));
	}

	public File getFile(String url) throws Exception {
		String path = url.replaceAll("\\\\", "/").replaceFirst("(?i)((http:/+)|(https:/+))", "").replaceFirst(".*?/",
				"/");
		File file = new File(config.webroot, path);
		return file;
	}

	public String newImage(InputStream is, String originalFileName, Integer quality) throws Exception {
		Connection connection = null;
		try {
			connection = config.dataSource.getConnection();
			return newImage(connection, is, originalFileName, quality);
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
			throw e;
		} finally {
			if (connection != null)
				connection.close();
		}
	}

	public String newImage(Connection connection, InputStream is, String originalFileName, Integer quality)
			throws Exception {
		PreparedStatement pst = null;
		StringBuilder sql = null;
		StringBuilder sql1 = null;
		StringBuilder sql2 = null;
		List sqlParams = null;
		boolean connAutocommitSrc = connection.getAutoCommit();
		try {
			if (connAutocommitSrc)
				connection.setAutoCommit(false);

			File projectFolder = new File(config.webroot + "/oss", config.project);
			logger.debug(projectFolder.getAbsolutePath());
			if (!projectFolder.exists())
				throw new RuntimeException("project not exist.");

			File tmpFolder = new File(projectFolder, "tmp");
			if (!tmpFolder.exists())
				tmpFolder.mkdirs();
			Date now = new Date();
			// 保存文件
			String url = null;
			sql = new StringBuilder(
					"insert into t_file (id,md5,name,size,path,linkedFileId,tmpIf) values(?,?,?,?,?,?,?)");
			sql1 = new StringBuilder("select id fileId,path,duration from t_file where  md5=? limit 1");

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

			if (quality != null && quality >= 1 && quality <= 9) {
				tmpFile.createNewFile();
				Builder builder = Thumbnails.of(is);
				builder.scale(1.0);
				builder.outputQuality(new Double(quality) / 10f);
				builder.toFile(tmpFile);
			} else {
				IOUtils.write(is, tmpFile);
			}
			md5 = IOUtils.getMD5(tmpFile);

			sqlParams = new ArrayList();
			sqlParams.add(md5);
			pst = connection.prepareStatement(sql1.toString());
			Map existFileRow = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst, sql1.toString(), sqlParams));
			pst.close();

			realFileId = existFileRow == null ? realFileId : ValueUtils.toString(existFileRow.get("fileId"));
			sqlParams = new ArrayList();
			sqlParams.add(linkFileId);
			sqlParams.add(null);
			sqlParams.add(linkFileName);
			sqlParams.add(tmpFile.length());
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
				sqlParams.add(realFileName);
				sqlParams.add(tmpFile.length());
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

			if (connAutocommitSrc)
				connection.commit();

			return url;
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
			if (connAutocommitSrc)
				connection.rollback();
			throw e;
		} finally {
			if (pst != null)
				pst.close();
			if (connAutocommitSrc)
				connection.setAutoCommit(true);
		}
	}

	public String newFile(Connection connection, InputStream is, String originalFileName, String cover,
			Integer duration) throws Exception {
		PreparedStatement pst = null;
		StringBuilder sql = null;
		StringBuilder sql1 = null;
		StringBuilder sql2 = null;
		List sqlParams = null;
		boolean connAutocommitSrc = connection.getAutoCommit();
		try {
			if (connAutocommitSrc)
				connection.setAutoCommit(false);

			File projectFolder = new File(config.webroot + "/oss", config.project);
			logger.debug(projectFolder.getAbsolutePath());
			if (!projectFolder.exists())
				throw new RuntimeException("project not exist.");

			File tmpFolder = new File(projectFolder, "tmp");
			if (!tmpFolder.exists())
				tmpFolder.mkdirs();
			Date now = new Date();
			// 保存文件
			String url = null;
			sql = new StringBuilder(
					"insert into t_file (id,md5,name,size,duration,cover,path,linkedFileId,tmpIf) values(?,?,?,?,?,?,?,?,?)");
			sql1 = new StringBuilder("select id fileId,path,duration from t_file where md5=? limit 1");

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
			sqlParams.add(md5);
			pst = connection.prepareStatement(sql1.toString());
			Map existFileRow = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst, sql1.toString(), sqlParams));
			pst.close();

			realFileId = existFileRow == null ? realFileId : ValueUtils.toString(existFileRow.get("fileId"));
			sqlParams = new ArrayList();
			sqlParams.add(linkFileId);
			sqlParams.add(null);
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

			if (connAutocommitSrc)
				connection.commit();

			return url;
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
			if (connAutocommitSrc)
				connection.rollback();
			throw e;
		} finally {
			if (pst != null)
				pst.close();
			if (connAutocommitSrc)
				connection.setAutoCommit(true);
		}
	}

	public String newFile(InputStream is, String originalFileName, String cover, Integer duration) throws Exception {
		Connection connection = null;
		try {
			connection = config.dataSource.getConnection();
			return newFile(connection, is, originalFileName, cover, duration);
		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
			throw e;
		} finally {
			if (connection != null)
				connection.close();
		}
	}
}
