package cn.dianhun.hadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 迁移一个表全部分区的数据
 *
 * @author GeZhiHui
 * @create 2018-07-17
 **/

public class TransWholeTablePartition {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransWholeTablePartition.class);

    private static final String SUBJECT = "数据迁移邮件报警";

    private static final Map<String, Long> srcFileToLen = new HashMap<>();
    private static final Map<String, Long> distFileToLen = new HashMap<>();

    public static void main(String[] args) {

        LOGGER.info("数据迁移开始");
        Options options = new Options();
        options.addOption("srcDB", true, "原DB");
        options.addOption("distDB", true, "目标DB");
        options.addOption("table", true, "表名");
        options.addOption("email", true, "接收告警邮箱");
        BasicParser parser = new BasicParser();

        CommandLine cl;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.error("解析参数异常 = {}", ExceptionUtils.getFullStackTrace(e));
            return;
        }

        String db = cl.getOptionValue("srcDB");
        if (CommonUtils.checkArgs(db, "缺少参数 srcDB")) return;
        String db2 = cl.getOptionValue("distDB");
        if (CommonUtils.checkArgs(db2, "缺少参数 distDB")) return;
        String table = cl.getOptionValue("table");
        if (CommonUtils.checkArgs(table, "缺少参数 table")) return;
        String email = cl.getOptionValue("email");
        if (CommonUtils.checkArgs(email, "缺少参数 email")) return;


        Configuration localConf = new Configuration();
        LocalFileSystem local = null;

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", KeyConstant.SRC_DEFAULT_FS);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs = null;

        Configuration conf2 = new Configuration();
        conf2.set("fs.defaultFS", KeyConstant.DIST_DEFAULT_FS);
        conf2.set("dfs.replication", "2");
        conf2.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs2 = null;
        try {
            local = FileSystem.getLocal(localConf);
            fs = FileSystem.get(new URI(KeyConstant.SRC_URL), conf, KeyConstant.HADOOP_USER);
            fs2 = FileSystem.get(new URI(KeyConstant.DIST_URL), conf2, KeyConstant.HADOOP_USER);

            List<String> partitions = new ArrayList<>();
            String path = KeyConstant.HDFS_COMMON_FILE_PATH + KeyConstant.FILE_SYSTEM_SEPARATOR + db + KeyConstant.FILE_SYSTEM_SEPARATOR + table;
            listAllPartitions(fs, path, partitions);
            //创建本地临时目录
            String localDir = KeyConstant.LOCAL_COMMON_FILE_PATH + KeyConstant.FILE_SYSTEM_SEPARATOR + db + KeyConstant.FILE_SYSTEM_SEPARATOR + table;
            if (!local.exists(new Path(localDir))) {
                local.mkdirs(new Path(localDir));
            }

            for (String p : partitions) {
                String src = KeyConstant.HDFS_COMMON_FILE_PATH + KeyConstant.FILE_SYSTEM_SEPARATOR + db + KeyConstant.FILE_SYSTEM_SEPARATOR + table + KeyConstant.FILE_SYSTEM_SEPARATOR + p;
                String localPath = KeyConstant.LOCAL_COMMON_FILE_PATH + KeyConstant.FILE_SYSTEM_SEPARATOR + db + KeyConstant.FILE_SYSTEM_SEPARATOR + table + KeyConstant.FILE_SYSTEM_SEPARATOR + p;
                String dist = KeyConstant.HDFS_COMMON_FILE_PATH + KeyConstant.FILE_SYSTEM_SEPARATOR + db2 + KeyConstant.FILE_SYSTEM_SEPARATOR + table + KeyConstant.FILE_SYSTEM_SEPARATOR + p;
                if (fs.exists(new Path(src))) {
                    //本地文件存在删除
                    if (local.exists(new Path(localPath))) {
                        local.delete(new Path(localPath), true);
                    }
                    CommonUtils.copyToLocalFile(fs, src, localPath);

                    if (fs2.exists(new Path(dist))) {
                        LOGGER.error("分区[{}]已经存在", dist);
                        CommonUtils.sendEmail(SUBJECT, "分区[" + dist + "]已经存在", email);
                        local.delete(new Path(localPath), true);
                        continue;
                    }
                    CommonUtils.copyFromLocalFile(fs2, localPath, dist);
                    // 复制完成，删除本地文件
                    if (local.exists(new Path(localPath))) {
                        local.delete(new Path(localPath), true);
                    }
                    //校验文件
                    CommonUtils.checkFile(fs, src, dist, email, SUBJECT, srcFileToLen, distFileToLen);
                }
            }
            LOGGER.info("数据迁移完成");
        } catch (Exception e) {
            LOGGER.error("数据迁移异常={}", ExceptionUtils.getFullStackTrace(e));
        } finally {
            try {
                if (local != null) {
                    local.close();
                }
            } catch (IOException e) {
                LOGGER.error("关闭LocalFileSystem异常={}", ExceptionUtils.getFullStackTrace(e));
            }
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                LOGGER.error("关闭fs异常={}", ExceptionUtils.getFullStackTrace(e));
            }
            try {
                if (fs2 != null) {
                    fs2.close();
                }
            } catch (IOException e) {
                LOGGER.error("关闭fs2异常={}", ExceptionUtils.getFullStackTrace(e));
            }
        }


    }

    /**
     * 获取所有的partition
     *
     * @param fs
     * @param path
     * @param partitions
     */
    private static void listAllPartitions(FileSystem fs, String path, List<String> partitions) throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isDirectory()) {
                partitions.add(fileStatus.getPath().getName());
            }
        }
    }

}
