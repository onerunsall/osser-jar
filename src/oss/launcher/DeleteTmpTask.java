package oss.launcher;

import java.io.File;
import java.io.FileFilter;
import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.giveup.IOUtils;
import com.giveup.UrlUtils;

class DeleteTmpTask implements Runnable {

	private static Logger logger = Logger.getLogger(DeleteTmpTask.class);

	private Config config;

	public DeleteTmpTask(Config config) {
		this.config = config;
	}

	public void run() {
		try {
			File root = new File(config.webroot, "oss");
			for (File projectRoot : root.listFiles()) {
				if (projectRoot.isDirectory()) {
					File tmp = new File(projectRoot, "tmp");
					if (tmp.exists()) {
						logger.debug("清理临时文件夹: " + tmp);
						if (tmp.exists() && tmp.isDirectory()) {
							tmp.listFiles(new FileFilter() {
								@Override
								public boolean accept(File file) {
									return ddd(file);
								}
							});

						}
					}
				}
			}

		} catch (Exception e) {
			logger.info(ExceptionUtils.getStackTrace(e));
		} finally {
		}
	}

	private boolean ddd(File file) {
		if (file.isDirectory()) {
			if (file.list().length == 0)
				return dddd(file);
			else {
				File[] files = file.listFiles(new FileFilter() {
					@Override
					public boolean accept(File file) {
						return ddd(file);
					}
				});
				if (file.list().length == 0)
					return dddd(file);
				else
					return false;
			}
		} else
			return dddd(file);

	}

	private boolean dddd(File file) {
		long time = file.lastModified();
		if ((new Date().getTime() - time) > 60 * 1000) {
			try {
				File linkFile = new File(file.getAbsolutePath().replaceAll("tmp", ""));
				logger.info("delete link " + linkFile.getAbsolutePath());
				linkFile.delete();

				File fileVariantFolder = new File(linkFile.getParent(), UrlUtils.fileExtStrip(linkFile.getName()));
				logger.info("delete link VariantFolder " + fileVariantFolder.getAbsolutePath());
				if (fileVariantFolder.exists()
						&& !fileVariantFolder.getAbsolutePath().equals(linkFile.getParentFile().getAbsolutePath()))
					if (!IOUtils.deleteRecursion(fileVariantFolder))
						throw new RuntimeException("删除变种文件夹失败");

				logger.info("delete file " + file.getAbsolutePath());
				return file.delete();
			} catch (Exception e) {
				logger.info(ExceptionUtils.getStackTrace(e));
				return false;
			}
		} else
			return false;
	}
}