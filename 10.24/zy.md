10月24日 韩天昊
# 10.24 安装kfk

mkdir -p /opt/pkg

mkdir -p /opt/soft

mkdir -p /data/logs/kraft-combined-logs

chmod a+rx /data/logs/kraft-combined-logs/

wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz

tar -zxvf kafka_2.13-3.4.0.tgz -C /opt/soft/

cd /opt/soft/kafka_2.13-3.4.0/config/kraft/

vim server.properties

cd /opt/soft/kafka_2.13-3.4.0/

#⽣成储存⽬录唯⼀ID
bin/kafka-storage.sh random-uuid
#控制台输出ZDJX-bKfTcy-BY4e-qmvTg 不会都⼀样

#⽤该ID格式化kafka储存⽬录
bin/kafka-storage.sh format -t ZDJX-bKfTcy-BY4e-qmvTg -c config/kraft/server.properties
#控制台输出Formatting /data/logs/kraft-combined-logs with metadata.version 3.4-IV0.

启动kafka
bin/kafka-server-start.sh -daemon config/kraft/server.properties

创建主题
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic zh_test --partitions 1 --
replication-factor 1



因为offsetexplorer 版本太低，不能匹配单kfk
，单kfk需要3.3及以上，我们当时安装的最大版本是3.1，
所以需要打开官网，安装最新版本即可
官网链接：https://code.visualstudio.com











