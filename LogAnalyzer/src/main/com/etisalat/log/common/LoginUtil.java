package com.etisalat.log.common;

import com.etisalat.log.config.LogConfFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LoginUtil {
    private static final Logger logger = LoggerFactory.getLogger(LoginUtil.class);

    private static final String JAAS_POSTFIX = ".jaas.conf";
    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    private static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String JAVA_SECURITY_LOGIN_CONF = "java.security.auth.login.config";
    private static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";

    public static void login() throws IOException {
        logger.info("start to login to the cluster.");

        // 1.check input parameters
        if ((LogConfFactory.userPrincipal == null) || (LogConfFactory.userPrincipal.length() <= 0)) {
            logger.error("input userPrincipal is invalid.");
            throw new IOException("input userPrincipal is invalid.");
        }

        if ((LogConfFactory.keytabFile == null) || (LogConfFactory.keytabFile.length() <= 0)) {
            logger.error("input userKeytabPath is invalid.");
            throw new IOException("input userKeytabPath is invalid.");
        }

        if ((LogConfFactory.krb5Conf == null) || (LogConfFactory.krb5Conf.length() <= 0)) {
            logger.error("input krb5ConfPath is invalid.");
            throw new IOException("input krb5ConfPath is invalid.");
        }

        if (LogConfFactory.hbaseConf == null) {
            logger.error("input conf is invalid.");
            throw new IOException("input conf is invalid.");
        }

        // 2.check file exsits
        File userKeytabFile = new File(LogConfFactory.keytabFile);
        if (!userKeytabFile.exists()) {
            logger.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile()) {
            logger.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
        }

        File krb5ConfFile = new File(LogConfFactory.krb5Conf);
        if (!krb5ConfFile.exists()) {
            logger.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!krb5ConfFile.isFile()) {
            logger.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
        }

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getAbsolutePath());
        setConfiguration(LogConfFactory.hbaseConf);

        // 4.login
        UserGroupInformation.loginUserFromKeytab(LogConfFactory.userPrincipal, userKeytabFile.getAbsolutePath());

        // 5. zk login
        setZookeeperServerPrincipal();
        setJaasFile(LogConfFactory.userPrincipal, LogConfFactory.keytabFile);

        logger.info("Login success!");
    }

    private static void setKrb5Config(String krb5ConfFile) throws IOException {
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF);
        if (ret == null) {
            logger.error(JAVA_SECURITY_KRB5_CONF + " is null.");
            throw new IOException(JAVA_SECURITY_KRB5_CONF + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            logger.error(JAVA_SECURITY_KRB5_CONF + " is " + ret + " is not " + krb5ConfFile + ".");
            throw new IOException(JAVA_SECURITY_KRB5_CONF + " is " + ret + " is not " + krb5ConfFile + ".");
        }

        logger.info("{}={}", JAVA_SECURITY_KRB5_CONF, ret);
    }

    private static void setZookeeperServerPrincipal() throws IOException {
        System.setProperty(ZOOKEEPER_AUTH_PRINCIPAL, LogConfFactory.zkPrinciple);
        String ret = System.getProperty(ZOOKEEPER_AUTH_PRINCIPAL);
        if (ret == null) {
            throw new IOException(ZOOKEEPER_AUTH_PRINCIPAL + " is null.");
        }
        if (!ret.equals(LogConfFactory.zkPrinciple)) {
            throw new IOException(
                    ZOOKEEPER_AUTH_PRINCIPAL + " is " + ret + " is not " + LogConfFactory.zkPrinciple + ".");
        }
        logger.info("{}={}", ZOOKEEPER_AUTH_PRINCIPAL, LogConfFactory.zkPrinciple);
    }

    private static void setConfiguration(Configuration conf) throws IOException {
        UserGroupInformation.setConfiguration(conf);
    }

    private static void setJaasFile(String principal, String keytabPath) throws IOException {
        String jaasPath = new File(LogConfFactory.configFilePath) + File.separator + System.getProperty("user.name")
                + JAAS_POSTFIX;
        jaasPath = jaasPath.replace("\\", "\\\\");
        keytabPath = keytabPath.replace("\\", "\\\\");

        deleteJaasFile(jaasPath);
        writeJaasFile(jaasPath, principal, keytabPath);
        System.setProperty(JAVA_SECURITY_LOGIN_CONF, jaasPath);
        logger.info("{}={}", JAVA_SECURITY_LOGIN_CONF, jaasPath);
    }

    private static void writeJaasFile(String jaasPath, String principal, String keytabPath) throws IOException {
        FileWriter writer = new FileWriter(new File(jaasPath));
        try {
            writer.write(getJaasConfContext(principal, keytabPath));
            writer.flush();
        } catch (IOException e) {
            throw new IOException("Failed to create jaas.conf File");
        } finally {
            writer.close();
        }
    }

    private static void deleteJaasFile(String jaasPath) throws IOException {
        File jaasFile = new File(jaasPath);
        if (jaasFile.exists()) {
            if (!jaasFile.delete()) {
                throw new IOException("Failed to delete exists jaas file.");
            }
        }
    }

    private static String getJaasConfContext(String principal, String keytabPath) {
        Module[] allModule = Module.values();
        StringBuilder builder = new StringBuilder();
        for (Module modlue : allModule) {
            builder.append(getModuleContext(principal, keytabPath, modlue));
        }
        return builder.toString();
    }

    private static String getModuleContext(String userPrincipal, String keyTabPath, Module module) {
        StringBuilder builder = new StringBuilder();
        if (IS_IBM_JDK) {
            builder.append(module.getName()).append(" {").append(LogConfFactory.lineSeparator);
            builder.append(IBM_LOGIN_MODULE).append(LogConfFactory.lineSeparator);
            builder.append("credsType=both").append(LogConfFactory.lineSeparator);
            builder.append("principal=\"" + userPrincipal + "\"").append(LogConfFactory.lineSeparator);
            builder.append("useKeytab=\"" + keyTabPath + "\"").append(LogConfFactory.lineSeparator);
            builder.append("debug=false;").append(LogConfFactory.lineSeparator);
            builder.append("};").append(LogConfFactory.lineSeparator);
        } else {
            builder.append(module.getName()).append(" {").append(LogConfFactory.lineSeparator);
            builder.append(SUN_LOGIN_MODULE).append(LogConfFactory.lineSeparator);
            builder.append("useKeyTab=true").append(LogConfFactory.lineSeparator);
            builder.append("keyTab=\"" + keyTabPath + "\"").append(LogConfFactory.lineSeparator);
            builder.append("principal=\"" + userPrincipal + "\"").append(LogConfFactory.lineSeparator);
            builder.append("useTicketCache=false").append(LogConfFactory.lineSeparator);
            builder.append("storeKey=true").append(LogConfFactory.lineSeparator);
            builder.append("debug=false;").append(LogConfFactory.lineSeparator);
            builder.append("};").append(LogConfFactory.lineSeparator);
        }

        return builder.toString();
    }

    private enum Module {
        STORM("StormClient"), KAFKA("KafkaClient"), ZOOKEEPER("Client");

        private String name;

        private Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}