安装准备：
1.下载并安装JDK,JDK版本使用1.8+如jdk1.8.0_51
2.下载并解压Apache Tomcat 7 or Tomcat 8; 如 /opt/apache-tomcat-7.0.63;
3.将WebRoot重名为LogAnalyzer，并拷贝到 /opt/apache-tomcat-7.0.63/webapps下；如/opt/apache-tomcat-7.0.63/webapps/LogAnalyzer
4.创建配置文件目录：/opt/apache-tomcat-7.0.63/logconf/
5.将dependent_jars_list列的jar文件拷贝到/opt/apache-tomcat-7.0.63/webapps/LogAnalyzer/WEB-INF/lib目录下
6.将scripts目录下的shell脚本拷贝到/opt/apache-tomcat-7.0.63/bin目录下，并执行 chmod u+x *.sh

脚本修改
#/opt/apache-tomcat-7.0.63/lbin/setenv.sh中将JAVA_HOME配置成jdk安装路径，JDK版本使用1.8+
export JAVA_HOME=/opt/huawei/Bigdata/jdk1.8.0_51/

配置：
1. 将HBase客户端的配置文件core-site.xml、hbase-site.xml、hdfs-site.xml拷贝到/opt/apache-tomcat-7.0.63/webapps/LogAnalyzer/WEB-INF/classes目录下
2.下载krb5.conf、应用账户keytab文件拷贝到tomcat安装路径logconf目录下；并修改修改/opt/apache-tomcat-7.0.63/logconf/logconf.properties对应配置
user.principal=
keytab.file.path=
krb5.conf.path=
3.修改tomcat安装路径/opt/apache-tomcat-7.0.63/logconf/logconf.properties
#zkHosts配置值可登录Solr Admin UI获取。
solr.zkHost=192.168.213.6:24002,192.168.213.5:24002,192.168.213.4:24002/solr
#IP替换成SolrAdmin的浮动IP
search.server.url=https://192.168.213.44:21101/solr/ 
