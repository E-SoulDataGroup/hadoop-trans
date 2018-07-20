## hadoop-trans
HDFS数据迁移、压缩工具。

## 项目编译打包
    gradle build -x test
    
    编译完成后jar包所在位置: hadoop-trans/build/libs/hadoop-trans-1.0-SNAPSHOT.jar

## 使用说明
* 类TransTablePartition适用于迁移某个库下面某个表一个时间内的分区数据
* 类TransWholeTablePartition适用于迁移某个库下面某个表全部分区数据
* 类HDFSMerge适用于合并压缩某个某个表一段时间内的分区数据，目前分区支持一级分区及二级分区

## 使用举例
    nohup java -cp hadoop-trans-1.0-SNAPSHOT.jar cn.dianhun.hadoop.TransTablePartition -srcDB m3gcn_test.db -distDB m3gcn_test.db -table tab_town_login -pn par_dt -pp yyyyMM -s 2017-03-01 -e 2017-04-30 -email gezhihui@dianhun.cn > run.log &

    java -cp hadoop-trans-1.0-SNAPSHOT.jar cn.dianhun.hadoop.TransTablePartition -srcDB m3gcn_data_log.db -distDB m3gcn_data_log12.db -table tab_zhizun_opr_log -pn par_dt -pp yyyyMM -s 2017-11-01 -e 2017-11-01 -email gezhihui@dianhun.cn
    
    java -cp hadoop-trans-1.0-SNAPSHOT.jar cn.dianhun.hadoop.TransWholeTablePartition -srcDB m3gcn_data_log.db -distDB m3gcn_data_log12.db -table tab_zhizun_opr_log -email gezhihui@dianhun.cn
    
    java -cp hadoop-trans-1.0-SNAPSHOT.jar cn.dianhun.hadoop.HDFSMerge -srcDB m3gcn_data_log.db -table tab_zhizun_opr_log -pn par_dt -pp yyyyMM -sp true -s 2017-11-01 -e 2017-11-30
    
    
## 参数说明
    java -cp 启动命令，后面跟随的是入口类
    
    -srcDB 迁出数据库
    
    -distDB 迁入数据库
    
    -table 表名
    
    -pn 分区名称
    
    -pp 分区pattern
    
    -sp 是否有二级分区，值为true,false
    
    -s 开始时间
    
    -e 结束时间
    
    -email 接收告警邮箱，每迁移完成一个分区就会做校验，不通过会发送告警邮件
    
    
    

