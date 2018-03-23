spark-2.2.0安装和部署——Spark集群学习日记
原创 2017年07月24日 17:03:03

    标签：
    spark /
    集群 /
    hadoop

前言

    在安装后hadoop之后，接下来需要安装的就是Spark。

scala-2.11.7下载与安装

具体步骤参见上一篇博文
Spark下载

为了方便，我直接是进入到了/usr/local文件夹下面进行下载spark-2.2.0

wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz

    1

Spark安装之前的准备

文件的解压与改名

tar -zxvf spark-2.2.0-bin-hadoop2.7.tgz

    1

rm -rf spark-2.2.0-bin-hadoop2.7.tgz

    1

为了我后面方便配置spark，在这里我把文件夹的名字给改了

mv spark-2.2.0-bin-hadoop2.7 spark-2.2.0

    1

配置环境变量

vi /etc/profile

    1

在最尾巴加入

export SPARK_HOME=/usr/local/spark-2.2.0

export PATH=$PATH:$SPARK_HOME/bin

    1
    2
    3

这里写图片描述
配置Spark环境

打开spark-2.2.0文件夹

cd spark-2.2.0

    1

此处需要配置的文件为两个
spark-env.sh和slaves

这里写图片描述

首先我们把缓存的文件spark-env.sh.template改为spark识别的文件spark-env.sh

cp conf/spark-env.sh.template conf /spark-env.sh

    1

修改spark-env.sh文件

vi conf/spark-env.sh

    1

在最尾巴加入

export JAVA_HOME=/usr/java/jdk1.8.0_141

export SCALA_HOME=/usr/scala-2.11.7

export HADOOP_HOME=/usr/local/hadoop-2.7.2

export HADOOP_CONF_DIR=/usr/local/hadoop-2.7.2/etc/hadoop

export SPARK_MASTER_IP=SparkMaster

export SPARK_WORKER_MEMORY=4g

export SPARK_WORKER_CORES=2

export SPARK_WORKER_INSTANCES=1

    1
    2
    3
    4
    5
    6
    7
    8
    9
    10
    11
    12
    13
    14
    15

变量说明
- JAVA_HOME：Java安装目录
- SCALA_HOME：Scala安装目录
- HADOOP_HOME：hadoop安装目录
- HADOOP_CONF_DIR：hadoop集群的配置文件的目录
- SPARK_MASTER_IP：spark集群的Master节点的ip地址
- SPARK_WORKER_MEMORY：每个worker节点能够最大分配给exectors的内存大小
- SPARK_WORKER_CORES：每个worker节点所占有的CPU核数目
- SPARK_WORKER_INSTANCES：每台机器上开启的worker节点的数目

这里写图片描述
修改slaves文件

vi conf/slaves

    1

在最后面修成为

SparkWorker1
SparkWorker2

    1
    2

这里写图片描述
同步SparkWorker1和SparkWorker2的配置

在此我们使用rsync命令

rsync -av /usr/local/spark-2.2.0/ SparkWorker1:/usr/local/spark-2.2.0/

    1

rsync -av /usr/local/spark-2.2.0/ SparkWorker2:/usr/local/spark-2.2.0/

    1

启动Spark集群

    因为我们只需要使用hadoop的HDFS文件系统，所以我们并不用把hadoop全部功能都启动。

启动hadoop的HDFS文件系统

start-dfs.sh

    1

    但是在此会遇到一个情况，就是使用start-dfs.sh，启动之后，在SparkMaster已经启动了namenode，但在SparkWorker1和SparkWorker2都没有启动了datanode，这里的原因是：datanode的clusterID和namenode的clusterID不匹配。是因为SparkMaster多次使用了hadoop namenode -format格式化了。

==解决的办法：==

在SparkMaster使用

cat /usr/local/hadoop-2.7.2/hdfs/name/current/VERSION

    1

查看clusterID，并将其复制。

这里写图片描述

在SparkWorker1和SparkWorker2上使用

vi /usr/local/hadoop-2.7.2/hdfs/name/current/VERSION

    1

将里面的clusterID，更改成为SparkMasterVERSION里面的clusterID

这里写图片描述

做了以上两步之后，便可重新使用start-dfs.sh开启HDFS文件系统。

这里写图片描述

启动之后使用jps命令可以查看到SparkMaster已经启动了namenode，SparkWorker1和SparkWorker2都启动了datanode，说明hadoop的HDFS文件系统已经启动了。

这里写图片描述

这里写图片描述

这里写图片描述
启动Spark

    因为hadoop/sbin以及spark/sbin均配置到了系统的环境中，它们同一个文件夹下存在同样的start-all.sh文件。最好是打开spark-2.2.0，在文件夹下面打开该文件。

./sbin/start-all.sh

    1

这里写图片描述

成功打开之后使用jps在SparkMaster、parkWorker1和SparkWorker2节点上分别可以看到新开启的Master和Worker进程。

这里写图片描述

这里写图片描述

这里写图片描述

成功打开Spark集群之后可以进入Spark的WebUI界面，可以通过

SparkMaster_IP:8080

    1

访问，可见有两个正在运行的Worker节点。

这里写图片描述
打开Spark-shell

使用

spark-shell

    1

这里写图片描述

便可打开Spark的shell

同时，因为shell在运行，我们也可以通过

SparkMaster_IP:4040

    1

访问WebUI查看当前执行的任务。

这里写图片描述
结言

    到此我们的Spark集群就搭建完毕了。搭建spark集群原来知识网络是挺庞大的，涉及到Linux基本操作，设计到ssh，设计到hadoop、Scala以及真正的Spark。在此也遇到不少问题，通过翻阅书籍以及查看别人的blog得到了解决。在此感谢分享知识的人。

    参见 王家林/王雁军/王家虎的《Spark 核心源码分析与开发实战》

文章出自kwongtai’blog，转载请标明出处！
版权声明：本文为博主kwongtai原创文章，如需转载，请标明出处，谢谢~ https://blog.csdn.net/weixin_36394852/article/details/76030317 
