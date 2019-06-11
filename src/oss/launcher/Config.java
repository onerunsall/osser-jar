package oss.launcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

public class Config {

	ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
	// OssLauncherTask ossLauncherTask = new OssLauncherTask(this);
	// RealizeTmpFileTask realizeTmpFileTask = new RealizeTmpFileTask(this);
	DeleteFileTask deleteFileTask = new DeleteFileTask(this);
	ClearTmpFolderTask deleteTmpTask = new ClearTmpFolderTask(this);
	DeleteNotrecordFileTask deleteNorecordFileTask = new DeleteNotrecordFileTask(this);
	DataSource dataSource = null;
	String project;
	String webroot;

	public Config(DataSource dataSource, String project, String webroot) {
		this.dataSource = dataSource;
		this.webroot = webroot;
		this.project = project;

		// scheduledExecutorService.execute(ossLauncherTask);
		// scheduledExecutorService.scheduleWithFixedDelay(realizeTmpFileTask, 0, 1,
		// TimeUnit.MINUTES);
		scheduledExecutorService.scheduleWithFixedDelay(deleteFileTask, 0, 1, TimeUnit.MINUTES);
		scheduledExecutorService.scheduleWithFixedDelay(deleteTmpTask, 0, 1, TimeUnit.MINUTES);
		scheduledExecutorService.execute(deleteNorecordFileTask);
	}

}
