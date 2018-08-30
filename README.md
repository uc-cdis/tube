# ETL process from postgres to ES

[![Build Status](https://travis-ci.com/uc-cdis/tube.svg?branch=feat/test-travis)](https://travis-ci.com/uc-cdis/tube)

We decided to use spark for running the ETL process, because:
1. It was used in our previous projects so we have familiarity with the framework and a certain level of confidence that it will work.
2. It scales very well with the data. We don't need to change any code when the data scale changes.
3. Good python API that's low learning curve for cdis dev teams.

We decided to use python instead of scala because cdis dev teams are much more comfortable with python programming. And since all the computation will be done in spark, we won't do any manipulation on the python level, the performance won't be a huge difference.

## Loading postgres data to spark RDD
Learning all the options that one of our collabators -OICR tried (posted [here](https://softeng.oicr.on.ca/grant_guo/2017/08/14/spark/) ). We decided to go with similar strategy - dump postgres to HDFS and load HDFS to rdd/SPARK.
We decided to use [SQOOP](https://github.com/apache/sqoop) to dump the postgres database to HDFS. In order to dump postgresql database, SQOOP calls [CopyManager](https://jdbc.postgresql.org/documentation/publicapi/org/postgresql/copy/CopyManager.html).

# Local Development Installation Guide
The installation guide below is only tested on OSX.

### Prerequisite

- Install Java and remember to configure JAVA\_HOME in the `~/.bash\_profile` or `~/.bashrc`

### Install HADOOP locally
Download the latest version of HADOOP to install HADOOP locally. Then, configure your local HADOOP as a psuedo cluster by following steps:

#### Configure environment variables

```
export HADOOP_HOME=YOUR_LOCAL_HADOOP_FOLDER/<VERSION_NUMBER>/libexec/
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
```

I installed HADOOP via `brew`, so my `HADOOP_HOME=/usr/local/Cellar/hadoop/3.1.0/libexec/`

#### Configure Hadoop configuration
You can find all the Hadoop configuration files in the location `$HADOOP_HOME/etc/hadoop`. You need to make suitable changes in those configuration files according to your Hadoop infrastructure.
* `core-site.xml`
```
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/usr/local/Cellar/hadoop/3.1.0/hdfs/tmp</value>
        <description>A base for other temporary directories.</description>
    </property>
    <property>
      <name>fs.default.name</name>
      <value>hdfs://localhost:9000</value>
   </property>
</configuration>
```
* `hdfs-site.xml`
```
<configuration>
   <property>
      <name>dfs.replication</name>
      <value>1</value>
   </property>
</configuration>
```
* `mapred-site.xml`
```
<configuration>
   <property>
      <name>yarn.nodemanager.aux-services</name>
      <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
</configuration>
```
* `yarn-site.xml`
```
<configuration>
<!-- Site specific YARN configuration properties -->
   <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
        <name>yarn.resourcemanager.address</name>
        <value>127.0.0.1:8032</value>
        </property>
        <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>127.0.0.1:8030</value>
        </property>
        <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>127.0.0.1:8031</value>
   </property>
</configuration>
```

* `hadoop-env.sh`
In this bash file, change the following line:
```
export JAVA_HOME=TO_YOUR_JAVA_HOME
export HADOOP_HOME=TO_YOUR_HADOOP_HOME
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug"
export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}
case ${HADOOP_OS_TYPE} in
  Darwin*)
    export HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.realm= "
    export HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.kdc= "
    export HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf= "
  ;;
esac
export HDFS_NAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS"
export HDFS_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS"
export HDFS_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS"
export HDFS_NAMENODE_USER=YOUR_USER_NODE_NAME
export HDFS_DATANODE_USER=YOUR_USER_NODE_NAME
export HDFS_SECONDATRYNAMENODE_USER=YOUR_USER_NODE_NAME
```

#### Enable SSH service locally
configure your environment to allow localhost ssh by adding the pub key to authorized list:
```
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

Check if you can ssh to the localhost without passphrase by running `ssh localhost`.

### Test HADOOP
Following this direction [HADOOP's direction](http://hadoop.apache.org/docs/r3.1.0/hadoop-project-dist/hadoop-common/SingleCluster.html#Execution)

### Install SPARK
Following this [link](https://medium.freecodecamp.org/installing-scala-and-apache-spark-on-mac-os-837ae57d283f) to install SPARK

### Install SQOOP
Download latest SQOOP here [SQOOP](http://sqoop.apache.org/)
```
tar -xvf sqoop-1.4.7.tar.gz
export SQOOP_HOME=YOUR_SQOOP_HOME_FOLDER
cd $SQOOP_HOME/conf
mv sqoop-env-template.sh sqoop-env.sh
```
Edit two following lines in `$SQOOP_HOME/sqoop-env.sh`:
```
export HADOOP_COMMON_HOME=YOUR_HADOOP_HOME 
export HADOOP_MAPRED_HOME=YOUR_HADOOP_HOME
```
Add sqoop path to your bash profile:
```
export SQOOP_HOME=YOUR_SCOPE_HOME
export PATH=$SQOOP_HOME/bin:$PATH
```

Download [postgresql jar file](https://jdbc.postgresql.org/download.html) to SQOOP_HOME/lib/

### Clone this repository to your local PC
`mv local_settings.example.py local_settings.py`

Change the configuration keys to suitable values.

Start hadoop if it's stopped: `$HADOOP_HOME/sbin/start-dfs.sh`
1. Run `python run_import.py` to import data from a SQL database to HDFS.
2. Run `python run_spark.py` to import data from the HDFS files to RDD.
