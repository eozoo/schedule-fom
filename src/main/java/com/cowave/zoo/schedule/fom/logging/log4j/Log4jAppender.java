package com.cowave.zoo.schedule.fom.logging.log4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Writer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.CountingQuietWriter;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.OptionConverter;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class Log4jAppender extends FileAppender{

	private static final int BUFFER = 8096;

	private static final long UNIT_DAY = 86400000;

	private static final long LOGDAY = 7;

	private static final long TARDAY = 180;

	private static final long FILESIZE = 20L * 1024 * 1024;

	private static final int MAXINDEX = 50;

	protected long maxFileSize = FILESIZE;

	protected int maxBackupIndex = MAXINDEX;

	private long logExistDay = LOGDAY;

	private long tarExistDay = TARDAY;

	private DateFormat datePattern;

	private boolean rolling = true;

	private long nextRollover = 0;

	public Log4jAppender() {

	}

	public Log4jAppender(Layout layout, String filename, boolean append) throws IOException {
		super(layout, filename, append);
	}

	public Log4jAppender(Layout layout, String filename) throws IOException {
		super(layout, filename);
	}


	public void setLogExistDay(long logExistDay) {
		this.logExistDay = logExistDay;
	}

	public void setTarExistDay(long tarExistDay) {
		this.tarExistDay = tarExistDay;
	}

	public void setDatePattern(String pattern) {
		try{
			this.datePattern = new SimpleDateFormat(pattern);
		}catch(Exception e){
			this.datePattern = new SimpleDateFormat("yyyyMMdd");
		}
	}

	public void setMaxBackupIndex(int maxBackups) {
		this.maxBackupIndex = maxBackups;
	}

	public void setMaximumFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public void setMaxFileSize(String value) {
		this.maxFileSize = OptionConverter.toFileSize(value, this.maxFileSize + 1L);
	}

	public void setRolling(String rolling) {
		try{
			this.rolling = Boolean.valueOf(rolling);
		}catch(Exception e){
			this.rolling = false;
		}
	}

	@Override
	protected void setQWForFiles(Writer writer) {
		this.qw = new CountingQuietWriter(writer, this.errorHandler);
	}

	@Override
	public synchronized void setFile(String fileName, boolean append, boolean bufferedIO, int bufferSize)
			throws IOException {
		super.setFile(fileName, append, this.bufferedIO, this.bufferSize);
		if (append) {
			File f = new File(fileName);
			((CountingQuietWriter) this.qw).setCount(f.length());
		}
	}

	@Override
	public void setFile(String file){
		super.setFile(file);
	}

	@Override
	protected void subAppend(LoggingEvent event) {
		if("\n".equals(event.getMessage())){
			this.qw.write("\n");
		}else{
			this.qw.write(this.layout.format(event));
		}

	    if(layout.ignoresThrowable()) {
	      String[] s = event.getThrowableStrRep();
	      if (s != null) {
		int len = s.length;
		for(int i = 0; i < len; i++) {
		  this.qw.write(s[i]);
		  this.qw.write(Layout.LINE_SEP);
		}
	      }
	    }
	    if(shouldFlush(event)) {
	      this.qw.flush();
	    }

		if (fileName != null && qw != null) {
			long size = ((CountingQuietWriter) qw).getCount();
			if(datePattern == null){
				datePattern = new SimpleDateFormat("yyyyMMdd");
			}
			String date = datePattern.format(System.currentTimeMillis());
			if(!rolling){
				if(size >= maxFileSize){
					rename(date);
				}
			}else{
				if (size >= maxFileSize && size >= nextRollover) {
					rollOver(date);
				}
			}
		}
	}

	public void rename(String date) {
		closeFile();

		File file = new File(fileName);
		File bak = new File(fileName + "." + date + "." + maxBackupIndex);
		//如果最大的日志备份序号已经存在，直接覆盖
		if(bak.exists()){
			try {
				if(bak.delete() && file.renameTo(bak)){
					setFile(fileName, false, bufferedIO, bufferSize);
				}else{
					setFile(fileName, true, bufferedIO, bufferSize);
				}
			} catch (IOException e) {
				if (e instanceof InterruptedIOException) {
					Thread.currentThread().interrupt();
				}
				LogLog.error("setFile(" + fileName + ", false) call failed.", e);
			}
			return;
		}

		int index = 1;
		for (; index <= maxBackupIndex; ++index) {
			bak = new File(fileName + "." + date + "." + index);
			if (!bak.exists()) {
				break;
			}
		}
		if(index == 1){
			new InnerThread(file.getParentFile(), fileName, logExistDay, tarExistDay).start();
		}

		//重命名
		try {
			if(file.renameTo(bak)){
				setFile(fileName, false, bufferedIO, bufferSize);
			}else{
				setFile(fileName, true, bufferedIO, bufferSize);
			}
		} catch (IOException e) {
			if (e instanceof InterruptedIOException) {
				Thread.currentThread().interrupt();
			}
			LogLog.error("setFile(" + fileName + ", false) call failed.", e);
		}
	}

	public void rollOver(String date) {
		File target = null;
		File file = null;

		if (qw != null) {
			long size = ((CountingQuietWriter) qw).getCount();
			LogLog.debug("rolling over count=" + size);
			nextRollover = size + maxFileSize;
		}
		LogLog.debug("maxBackupIndex="+maxBackupIndex);

		boolean renameSucceeded = true;
		if(maxBackupIndex > 0) {
			file = new File(fileName + '.' + date + '.' + maxBackupIndex);
			if (file.exists())
				renameSucceeded = file.delete();

			for (int i = maxBackupIndex - 1; i >= 1 && renameSucceeded; i--) {
				file = new File(fileName + '.' + date + "." + i);
				if (file.exists()) {
					target = new File(fileName + '.' + date + '.' + (i + 1));
					LogLog.debug("Renaming file " + file + " to " + target);
					renameSucceeded = file.renameTo(target);
				}
			}

			if(renameSucceeded) {
				target = new File(fileName + '.' + date + "." + 1);
				this.closeFile(); // keep windows happy.

				file = new File(fileName);
				LogLog.debug("Renaming file " + file + " to " + target);
				renameSucceeded = file.renameTo(target);

				if (!renameSucceeded) {
					try {
						this.setFile(fileName, true, bufferedIO, bufferSize);
					}
					catch(IOException e) {
						if (e instanceof InterruptedIOException) {
							Thread.currentThread().interrupt();
						}
						LogLog.error("setFile("+fileName+", true) call failed.", e);
					}
				}
			}
		}

		if (renameSucceeded) {
			if(file != null){
				new InnerThread(file.getParentFile(), fileName, logExistDay, tarExistDay).start();
			}
			try {
				this.setFile(fileName, false, bufferedIO, bufferSize);
				nextRollover = 0;
			}
			catch(IOException e) {
				if (e instanceof InterruptedIOException) {
					Thread.currentThread().interrupt();
				}
				LogLog.error("setFile("+fileName+", false) call failed.", e);
			}
		}
	}


	private static class InnerThread extends Thread {

		private File logDir;

		private String logName;

		private long logExistDay = TARDAY;

		private long tarExistDay = TARDAY;

		public InnerThread(File logDir, String logName, long logExistDay, long tarExistDay){
			this.logDir = logDir;
			this.logName = logName;
			this.logExistDay = logExistDay;
			this.tarExistDay = tarExistDay;
		}


		@Override
		public void run() {
			File[] array = logDir.listFiles();
			if(array == null){
				return;
			}

			long now = System.currentTimeMillis();
			DateFormat format = new SimpleDateFormat("yyyyMMdd");

			Map<String, List<File>> logMap = new HashMap<String, List<File>>();
			for(File file : array){
				String name = file.getName();
				if(name.equals(logName) || !(name.startsWith(logName))) {
					continue;
				}

				if(name.endsWith(".tar.gz")){
					name = name.replace(".tar.gz", "");
					String date = name.substring(name.lastIndexOf(".") + 1);
					try {
						long time = format.parse(date).getTime();
						long existDay = (now - time) / UNIT_DAY;
						if(existDay > tarExistDay){
							file.delete();
						}
					} catch (ParseException e) {
						LogLog.error("", e);
					}
					continue;
				}

				name = name.substring(0, name.lastIndexOf("."));
				String date = name.substring(name.lastIndexOf(".") + 1);
				try {
					long time = format.parse(date).getTime();
					long existDay = (now - time) / UNIT_DAY;
					if(existDay <= logExistDay){
						continue;
					}

					List<File> logList = logMap.get(date);
					if(logList == null){
						logList = new ArrayList<File>();
						logMap.put(date, logList);
					}
					logList.add(file);
				} catch (ParseException e) {
					LogLog.error("", e);
				}
			}

			for(Entry<String, List<File>> entry : logMap.entrySet()){
				String date = entry.getKey();
				List<File> logList = entry.getValue();
				targiz(logList, logName + "." + date);
			}
		}

		private void targiz(List<File> files, String fileName) {
			fileName = fileName + ".tar";
			File tar = new File(logDir + File.separator + fileName);
			TarArchiveOutputStream tarOut = null;
			try{
				tarOut = new TarArchiveOutputStream(new FileOutputStream(tar));
				for (File file : files) {
					FileInputStream input = null;
					try{
						input = new FileInputStream(file);
						tarOut.putArchiveEntry(new TarArchiveEntry(file));
						IOUtils.copy(input, tarOut);
						tarOut.closeArchiveEntry();
						tarOut.flush();
					}finally{
						close(input);
					}
				}
			}catch(Exception e) {
				LogLog.error("", e);
				return;
			}finally{
				close(tarOut);
			}

			fileName = fileName + ".gz";
			File gzip = new File(logDir + File.separator + fileName);
			FileInputStream in = null;
			GZIPOutputStream gzipOut = null;
			try {
				in = new FileInputStream(tar);
				gzipOut = new GZIPOutputStream(new FileOutputStream(gzip));
				byte[] array = new byte[BUFFER];
				int number = -1;
				while((number = in.read(array, 0, array.length)) != -1) {
					gzipOut.write(array, 0, number);
				}
			}catch(Exception e){
				LogLog.error("", e);
				return;
			}finally{
				close(in);
				close(gzipOut);
			}
			tar.delete();
			for (File file : files) {
				file.delete();
			}
		}
	}

	private static void close(AutoCloseable con){
		if(con == null){
			return;
		}
		try{
			con.close();
		}catch(Exception e){

		}
	}
}
