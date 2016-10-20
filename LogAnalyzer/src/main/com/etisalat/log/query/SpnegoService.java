package com.etisalat.log.query;

import com.etisalat.log.common.JsonUtil;
import com.etisalat.log.common.LogQueryException;
import com.google.gson.JsonObject;
import net.sourceforge.spnego.SpnegoHttpURLConnection;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import javax.security.auth.login.LoginException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SpnegoService {
    protected static final Logger logger = LoggerFactory.getLogger(SpnegoService.class);

    static {
        disableSslVerification();
    }

    private SpnegoHttpURLConnection spnego = null;
    private HttpURLConnection httpURLConn = null;

    public SpnegoService() {
        try {
            spnego = new SpnegoHttpURLConnection("Client", "any", "any");
        } catch (LoginException e) {
            logger.error(e.getMessage());
        }
    }

    private static void disableSslVerification() {
        try {
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }
            } };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Create all-trusting host name verifier
            HostnameVerifier allHostsValid = new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            };

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        } catch (KeyManagementException e) {
            logger.error(e.getMessage());
        }
    }

    public BufferedReader request(String url) throws LogQueryException {
        if (null == spnego) {
            logger.error("spnego is null, please initiated firstly!");
            //return null;
            throw new LogQueryException("spnego is null, please initiated firstly!", 509);
        }

        logger.debug("url : {}", url);
        BufferedReader br = null;
        try {
            httpURLConn = spnego.connect(new URL(url));

            int rspCode = httpURLConn.getResponseCode();
            logger.debug("ResponseCode : {}", rspCode);

            InputStream is = null;
            if (404 != rspCode) {
                if (rspCode >= 200 && rspCode < 400) {
                    is = httpURLConn.getInputStream();
                    br = new BufferedReader(new InputStreamReader(is));
                } else {
                    is = httpURLConn.getErrorStream();
                    br = new BufferedReader(new InputStreamReader(is));
                    String line;
                    StringBuilder queryResponse = new StringBuilder();

                    while ((line = br.readLine()) != null) {
                        queryResponse.append(line);
                    }

                    this.shutdown();

                    JsonObject rspJsonObj = JsonUtil.fromJson(queryResponse.toString(), JsonObject.class);
                    JsonObject errorOjb = rspJsonObj.getAsJsonObject("error");

                    throw new LogQueryException(errorOjb.get("msg").getAsString(), rspCode);
                }
            } else {
                this.shutdown();
                throw new LogQueryException("Not Found!", 404);
            }
        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
            throw new LogQueryException(e.getMessage(), 400);
        } catch (GSSException e) {
            logger.error(e.getMessage());
            throw new LogQueryException(e.getMessage(), 400);
        } catch (PrivilegedActionException e) {
            logger.error(e.getMessage());
            throw new LogQueryException(e.getMessage(), 400);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new LogQueryException(e.getMessage(), 400);
        }

        return br;
    }

    public void shutdown() {
        if (null != spnego) {
            spnego.disconnect();
        }
    }
}
