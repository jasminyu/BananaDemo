package com.etisalat.download;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class CleanDownloadThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CleanDownloadThread.class);

    private static final String FILE_SUFFIX = "tar.gz";

    private String exportLocalPath = null;

    private long timeInterval = 0;

    private long sleepTime = 0;

    public CleanDownloadThread(String exportLocalPath, long timeInterval, long sleepTime) {
        this.exportLocalPath = exportLocalPath;
        this.timeInterval = timeInterval;
        this.sleepTime = sleepTime;
    }

    private static void deleteDirectory(String sPath, long timeInterval) {
        if (!sPath.endsWith(File.separator)) {
            sPath = sPath + File.separator;
        }

        File dirFile = new File(sPath);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return;
        }

        long nowTimestamp = new Date().getTime();
        File[] files = dirFile.listFiles();
        File file = null;
        for (int i = 0; i < files.length; i++) {
            file = files[i];
            if (isExporttedFile(file, nowTimestamp, timeInterval)) {
                if (!deleteFile(getCanonicalPath(files[i]))) {
                    LOG.warn("failed to delete file {}", getCanonicalPath(file));
                }

            }
        }
    }

    private static boolean isExporttedFile(File file, long nowTimestamp, long timeInterval) {
        if (!file.isFile()) {
            return false;
        }

        long timestamp = file.lastModified();
        boolean isExpired = (nowTimestamp - timestamp) >= timeInterval;
        if (file.getName().endsWith(FILE_SUFFIX) && isExpired) {
            return true;
        }
        return false;
    }

    /**
     * delete single file
     *
     * @param sPath the deleted file path
     * @return the single file deleted successfully return true, otherwise
     * returns false
     */
    private static boolean deleteFile(String sPath) {
        boolean flag = false;
        File file = new File(sPath);

        if ((file.isFile()) && (file.exists())) {
            try {
                file.delete();
            } catch (SecurityException e) {
                LOG.error(e.toString());
                return false;
            }

            flag = true;
        }

        return flag;
    }

    private static String getCanonicalPath(File file) {
        if (file != null) {
            try {
                return file.getCanonicalPath();
            } catch (IOException ioe) {
                LOG.error("Fail to get path for file: ", file.getName());
            }
        }
        return StringUtils.EMPTY;
    }

    public static void main(String[] args) {
        System.out.println();
    }

    @Override
    public void run() {
        LOG.debug("start to clean expired files");
        while (true) {
            try {
                deleteDirectory(exportLocalPath, timeInterval);
                Thread.sleep(sleepTime);
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }
    }
}
