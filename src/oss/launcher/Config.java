package oss.launcher;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
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
	DeleteNotrecordFileTask deleteNotrecordFileTask = new DeleteNotrecordFileTask(this);
	DeleteNotexistFileTask deleteNotexistFileTask = new DeleteNotexistFileTask(this);
	DataSource dataSource = null;
	String project;
	String webroot;

	public Config(DataSource dataSource, String project, String webroot) throws ParseException {
		this.dataSource = dataSource;
		this.webroot = webroot;
		this.project = project;

		// scheduledExecutorService.execute(ossLauncherTask);
		// scheduledExecutorService.scheduleWithFixedDelay(realizeTmpFileTask, 0, 1,
		// TimeUnit.MINUTES);
		scheduledExecutorService.scheduleWithFixedDelay(deleteTmpTask, 0, 10, TimeUnit.MINUTES);
		scheduledExecutorService.scheduleWithFixedDelay(deleteFileTask, 0, 1, TimeUnit.MINUTES);
		scheduledExecutorService.scheduleWithFixedDelay(deleteNotexistFileTask, 0, 5, TimeUnit.MINUTES);

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 23);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		Timer timer = new Timer();
		timer.scheduleAtFixedRate((new TimerTask() {
			@Override
			public void run() {
				deleteNotrecordFileTask.run();
			}
		}), calendar.getTime(), 48 * 60 * 60 * 1000l);
	}

}
