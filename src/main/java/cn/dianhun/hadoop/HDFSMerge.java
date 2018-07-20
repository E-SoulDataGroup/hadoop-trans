package cn.dianhun.hadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyHadoopCompatibleOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;


/**
 * 合并压缩文件
 *
 * @author GeZhiHui
 * @create 2018-07-17
 **/

public class HDFSMerge {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSMerge.class);

    public static void main(String[] args) {

        LOGGER.info("数据开始合并开始");

        org.apache.commons.cli.Options options = new Options();
        options.addOption("srcDB", true, "原DB");
        options.addOption("table", true, "表名");
        options.addOption("pn", true, "分区名称");
        options.addOption("pp", true, "分区样式");
        options.addOption("sp", true, "是否有二级分区");
        options.addOption("s", true, "开始时间");
        options.addOption("e", true, "结束时间");

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
        String table = cl.getOptionValue("table");
        if (CommonUtils.checkArgs(table, "缺少参数 table")) return;
        String partitionName = cl.getOptionValue("pn");
        if (CommonUtils.checkArgs(partitionName, "缺少参数 pn")) return;
        String partitionPattern = cl.getOptionValue("pp");
        if (CommonUtils.checkArgs(partitionPattern, "缺少参数 pp")) return;
        boolean secondaryPartition = Boolean.parseBoolean(cl.getOptionValue("sp"));
        if (CommonUtils.checkArgs(cl.getOptionValue("sp"), "缺少参数 sp")) return;
        String start = cl.getOptionValue("s");
        if (CommonUtils.checkArgs(start, "缺少参数 s")) return;
        String end = cl.getOptionValue("e");
        if (CommonUtils.checkArgs(end, "缺少参数 e")) return;

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", KeyConstant.SRC_DEFAULT_FS);
        conf.set("dfs.replication", "2");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs = null;

        try {
            fs = FileSystem.get(new URI(KeyConstant.SRC_URL), conf, KeyConstant.HADOOP_USER);

            Set<String> partitions = CommonUtils.listPartitions(start, end, partitionName, partitionPattern);
            List<String> partitionsTime = CommonUtils.listPartitionsTime(start, end);
            Map<String, List<String>> partitionsToDate = CommonUtils.listPartitionsToDate(partitions, partitionsTime);
            String src = KeyConstant.HDFS_COMMON_FILE_PATH + KeyConstant.FILE_SYSTEM_SEPARATOR + db + KeyConstant.FILE_SYSTEM_SEPARATOR + table;
            for (String partition : partitions) {
                List<String> pt = partitionsToDate.get(partition);
                Map<String, List<Path>> ptToName = new HashMap<>();
                for (String time : pt) {
                    ptToName.put(time, new ArrayList<>());
                }
                if (secondaryPartition) {
                    String path = src + KeyConstant.FILE_SYSTEM_SEPARATOR + partition;
                    if (fs.exists(new Path(path))) {
                        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
                        for (FileStatus fileStatus : fileStatuses) {
                            if (fileStatus.isDirectory()) {
                                String path2 = path + KeyConstant.FILE_SYSTEM_SEPARATOR + fileStatus.getPath().getName();
                                RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path2), true);
                                listNeedCompressFile(pt, ptToName, listFiles);
                                //合并文件
                                compressFile(fs, table, src, ptToName, path2);
                            }
                        }
                    }
                } else {
                    String path = src + KeyConstant.FILE_SYSTEM_SEPARATOR + partition;
                    if (fs.exists(new Path(path))) {
                        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path), true);
                        listNeedCompressFile(pt, ptToName, listFiles);
                        //合并文件
                        compressFile(fs, table, src, ptToName, path);
                    }
                }
            }
            LOGGER.info("数据合并压缩结束");
        } catch (Exception e) {
            LOGGER.error("合并文件异常={}", ExceptionUtils.getFullStackTrace(e));
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                LOGGER.error("关闭fs异常={}", ExceptionUtils.getFullStackTrace(e));
            }
        }

    }

    /**
     * 合并压缩文件
     *
     * @param fs
     * @param table
     * @param src
     * @param ptToName
     * @param path2
     * @throws IOException
     */
    private static void compressFile(FileSystem fs, String table, String src, Map<String, List<Path>> ptToName, String path2) throws IOException {
        for (Map.Entry<String, List<Path>> entry : ptToName.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                boolean flag = copyMerge(fs, new Path(src), fs, new Path(path2 + KeyConstant.FILE_SYSTEM_SEPARATOR + table + "_" + entry.getKey() + ".snappy"), entry.getValue());
                //合并完成之后删除源文件
                if (flag) {
                    for (Path ph : entry.getValue()) {
                        fs.deleteOnExit(ph);
                    }
                }
            }
        }
    }

    /**
     * 获取需要压缩的文件
     *
     * @param pt
     * @param ptToName
     * @param listFiles
     * @throws IOException
     */
    private static void listNeedCompressFile(List<String> pt, Map<String, List<Path>> ptToName, RemoteIterator<LocatedFileStatus> listFiles) throws IOException {
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus2 = listFiles.next();
            String name = fileStatus2.getPath().getName();
            for (String s : pt) {
                if (StringUtils.contains(name, s) && !StringUtils.contains(name, "snappy")) {
                    List<Path> list = ptToName.get(s);
                    list.add(fileStatus2.getPath());
                    ptToName.put(s, list);
                }
            }
        }
    }

    /**
     * 合并文件
     *
     * @param srcFS
     * @param srcDir
     * @param dstFS
     * @param dstFile
     * @param paths
     * @return
     * @throws IOException
     */
    private static boolean copyMerge(FileSystem srcFS,
                                     Path srcDir,
                                     FileSystem dstFS,
                                     Path dstFile,
                                     List<Path> paths) throws IOException {
        if (!srcFS.getFileStatus(srcDir).isDirectory()) {
            return false;
        }

        OutputStream out = dstFS.create(dstFile);
        //snappy压缩使用SnappyHadoopCompatibleOutputStream，其他的压缩hive不能解压
        SnappyHadoopCompatibleOutputStream snappyOut = new SnappyHadoopCompatibleOutputStream(out);
        try {
            for (int i = 0; i < paths.size(); i++) {
                InputStream in = srcFS.open(paths.get(i));
                try {
                    byte[] buf = new byte[4096];
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        snappyOut.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    in.close();
                }
            }
        } finally {
            out.flush();
            snappyOut.flush();
            snappyOut.close();
            out.close();
        }
        return true;
    }


}
