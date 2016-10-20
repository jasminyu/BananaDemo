/*
 * Copyright Notice:
 *      Copyright  1998-2013, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.etisalat.download;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * tar.gz type file compression processing class.
 */
public class TarGZCompress {
    private static final Logger LOG = LoggerFactory.getLogger(TarGZCompress.class);

    private static final String SLASH = "/";

    /**
     * generate *.tar.gz type file.
     *
     * @param dirPath
     * @param tarGzPath
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void createTarGZ(String dirPath, String tarGzPath) throws IOException {
        FileOutputStream fOut = null;
        BufferedOutputStream bOut = null;
        GzipCompressorOutputStream gzOut = null;
        TarArchiveOutputStream tOut = null;
        try {
            fOut = new FileOutputStream(new File(tarGzPath));
            bOut = new BufferedOutputStream(fOut);
            gzOut = new GzipCompressorOutputStream(bOut);
            tOut = new TarArchiveOutputStream(gzOut);
            tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

            addFileToTarGz(tOut, dirPath, "");
        } catch (FileNotFoundException e) {
            LOG.error("System can not find the path specified.");
        } finally {
            if (null != tOut) {
                tOut.flush();
            }

            sync(fOut);

            if (null != tOut) {
                try {
                    tOut.finish();
                } catch (IOException e) {
                    LOG.error(StringUtils.EMPTY, e);
                }

                try {
                    tOut.close();
                } catch (IOException e) {
                    LOG.error(StringUtils.EMPTY, e);
                }
            }

            if (null != gzOut) {
                try {
                    gzOut.close();
                } catch (IOException e) {
                    LOG.error(StringUtils.EMPTY, e);
                }
            }

            if (null != bOut) {
                try {
                    bOut.close();
                } catch (IOException e) {
                    LOG.error(StringUtils.EMPTY, e);
                }
            }

            if (null != fOut) {
                try {
                    fOut.close();
                } catch (IOException e) {
                    LOG.error(StringUtils.EMPTY, e);
                }
            }
        }
    }

    private static void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base) throws IOException {
        File file = new File(path);
        String entryName = base + file.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry(file, entryName);

        // tarEntry.setMode must be put before tOut.putArchiveEntry operation,
        // otherwise it does not take effect
        tarEntry.setMode(getFileMode(file));
        tOut.putArchiveEntry(tarEntry);

        if (file.isFile()) {
            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                IOUtils.copy(inputStream, tOut);
            } finally {
                if (null != inputStream) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        LOG.error("Failed to close Input Stream", e);
                    }
                }

                closeEntry(tOut);
            }
        } else {
            closeEntry(tOut);

            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addFileToTarGz(tOut, child.getCanonicalPath(), entryName + SLASH);
                }
            }
        }
    }

    /**
     * close entry file handler
     *
     * @param tOut
     */
    private static void closeEntry(TarArchiveOutputStream tOut) {
        try {
            tOut.closeArchiveEntry();
        } catch (IOException e) {
            LOG.error("Failed to close Archive Entry", e);
        }
    }

    public static void sync(OutputStream os) {

        if (null != os && os instanceof FileOutputStream) {
            FileOutputStream fos = (FileOutputStream) os;

            try {
                fos.flush();
            } catch (IOException e) {
                LOG.warn("flush OutputStream error");
            }

            try {
                FileDescriptor fd = fos.getFD();
                fd.sync();
            } catch (IOException e) {
                LOG.warn("sync OutputStream error");
            }
        } else {
            LOG.warn("input OutputStream instance is null");
        }
    }

    private static int getFileMode(File file) {
        Set<PosixFilePermission> permissionSet;
        try {
            permissionSet = Files
                    .getPosixFilePermissions(Paths.get(file.getCanonicalPath(), new String[0]), new LinkOption[0]);
        } catch (IOException | UnsupportedOperationException e) {
            LOG.warn("Failed to convert file mode.", e);
            return 700;
        }
        String mode = PosixFilePermissions.toString(permissionSet);
        char[] m = mode.toCharArray();
        int sum = 0;
        int bit = 1;
        for (int i = m.length - 1; i >= 0; i--) {
            if (m[i] != '-') {
                sum += bit;
            }
            bit += bit;
        }
        return sum;
    }

    /**
     * create temporary directory
     *
     * @param path
     * @return
     */
    public static boolean createDir(String path) {
        File file = new File(path);
        if (!file.mkdirs()) {
            LOG.error("Failed to create dir {}.", path);
            return false;
        }
        return true;
    }

    /**
     * delete the directory and files under the directory
     *
     * @param sPath the deleted directory file path
     * @return directory deleted successfully return true, otherwise returns
     * false
     */
    public static boolean deleteDirectory(String sPath) {
        if (!sPath.endsWith(File.separator)) {
            sPath = sPath + File.separator;
        }

        File dirFile = new File(sPath);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return false;
        }

        boolean flag = true;
        File[] files = dirFile.listFiles();
        if (files == null || files.length == 0) {
            return true;
        }
        for (File file : files) {
            if (file.isFile()) {
                flag = deleteFile(getCanonicalPath(file));
                if (!flag) {
                    break;
                }
            } else {
                flag = deleteDirectory(getCanonicalPath(file));
                if (!flag) {
                    break;
                }
            }
        }

        return flag && dirFile.delete();
    }

    /**
     * delete single file
     *
     * @param sPath the deleted file path
     * @return the single file deleted successfully return true, otherwise
     * returns false
     */
    public static boolean deleteFile(String sPath) {
        File file = new File(sPath);

        if ((file.isFile()) && (file.exists())) {
            try {
                return file.delete();
            } catch (SecurityException e) {
                LOG.error(e.toString());
                return false;
            }
        }
        return true;
    }

    public static String getCanonicalPath(File file) {
        if (file != null) {
            try {
                return file.getCanonicalPath();
            } catch (IOException ioe) {
                LOG.error("Fail to get path for file: ", file.getName());
            }
        }
        return StringUtils.EMPTY;
    }
}
