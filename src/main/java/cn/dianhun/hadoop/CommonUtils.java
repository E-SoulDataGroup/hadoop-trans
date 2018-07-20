package cn.dianhun.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * CommonUtils
 *
 * @author GeZhiHui
 * @create 2018-07-17
 **/

public class CommonUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtils.class);

    private static final String PARTITION_SEPARATOR = "=";
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String DATE_PATTERN_2 = "yyyyMMdd";

    private CommonUtils() {
    }

    /**
     * 参数校验
     *
     * @param arg
     * @param msg
     * @return
     */
    public static boolean checkArgs(String arg, String msg) {
        if (StringUtils.isBlank(arg)) {
            LOGGER.error(msg);
            return true;
        }
        return false;
    }

    /**
     * copy文件到本地
     *
     * @param fs
     * @param src
     * @param dist
     * @throws IOException
     */
    public static void copyToLocalFile(FileSystem fs, String src, String dist) throws IOException {
        fs.copyToLocalFile(false, new Path(src), new Path(dist), true);
    }

    /**
     * 从本地copy文件到文件系统
     *
     * @param fs
     * @param src
     * @param dist
     */
    public static void copyFromLocalFile(FileSystem fs, String src, String dist) throws IOException {
        fs.copyFromLocalFile(new Path(src), new Path(dist));
    }

    /**
     * 发送邮件
     *
     * @param emailMsg
     * @param email
     */
    public static void sendEmail(String subject, String emailMsg, String email) {
        if (StringUtils.isNotBlank(email)) {
            SendEmail.sendEmail(subject, emailMsg, email);
        }
    }


    /**
     * 文件校验
     *
     * @param fs
     * @param src
     * @param dist
     * @param email
     * @param subject
     * @param srcFileToLen
     * @param distFileToLen
     * @throws IOException
     */
    public static void checkFile(FileSystem fs, String src, String dist, String email, String subject, Map<String, Long> srcFileToLen, Map<String, Long> distFileToLen) throws IOException {
        listFile(fs, src, srcFileToLen);
        listFile(fs, dist, distFileToLen);
        if (srcFileToLen.size() != distFileToLen.size()) {
            LOGGER.error("文件个数不匹配={}", src);
            String emailMsg = "文件个数不匹配,分区是[" + src + "]";
            CommonUtils.sendEmail(subject, emailMsg, email);
        }
        for (Map.Entry<String, Long> entry : srcFileToLen.entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            if (distFileToLen.containsKey(key)) {
                if (!Objects.equals(value, distFileToLen.get(key))) {
                    LOGGER.error("分区 = {}，文件大小不匹配 = {}", src, key);
                    String emailMsg = "文件大小不匹配,分区是[" + src + "],文件名是[" + key + "]";
                    CommonUtils.sendEmail(subject, emailMsg, email);
                }
            } else {
                LOGGER.error("分区 = {}，缺少文件 = {}", src, key);
                String emailMsg = "缺少文件,分区是[" + src + "],缺少文件[" + key + "]";
                CommonUtils.sendEmail(subject, emailMsg, email);
            }
        }
        srcFileToLen.clear();
        distFileToLen.clear();
    }

    /**
     * 获取所有文件
     *
     * @param fs
     * @param path
     * @param fileToLen
     * @throws IOException
     */
    private static void listFile(FileSystem fs, String path, Map<String, Long> fileToLen) throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            fileToLen.put(fileStatus.getPath().getName(), fileStatus.getLen());
        }
    }

    /**
     * 获取一段时间内的分区
     *
     * @param start
     * @param end
     * @param partitionName
     * @param partitionPattern
     * @return
     */
    public static Set<String> listPartitions(String start, String end, String partitionName, String partitionPattern) {
        Set<String> partitions = new TreeSet<>();
        DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_PATTERN);
        DateTime startDateTime = DateTime.parse(start, formatter);
        DateTime endDateTime = DateTime.parse(end, formatter);
        int days = Days.daysBetween(startDateTime, endDateTime).getDays();
        for (int i = 0; i <= days; i++) {
            DateTime dateTime = startDateTime.plusDays(i);
            String s = dateTime.toString(partitionPattern);
            partitions.add(partitionName + PARTITION_SEPARATOR + s);
        }
        return partitions;
    }

    /**
     * 获取所有分区的时间
     *
     * @param start
     * @param end
     * @return
     */
    public static List<String> listPartitionsTime(String start, String end) {
        List<String> partitionsDate = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormat.forPattern(DATE_PATTERN);
        DateTime startDateTime = DateTime.parse(start, formatter);
        DateTime endDateTime = DateTime.parse(end, formatter);
        int days = Days.daysBetween(startDateTime, endDateTime).getDays();
        for (int i = 0; i <= days; i++) {
            DateTime dateTime = startDateTime.plusDays(i);
            partitionsDate.add(dateTime.toString(DATE_PATTERN_2));
        }
        return partitionsDate;
    }

    /**
     * 分区与分区包含时间的映射
     *
     * @param partitions
     * @param partitionTime
     * @return
     */
    public static Map<String, List<String>> listPartitionsToDate(Set<String> partitions, List<String> partitionTime) {
        Map<String, List<String>> map = new HashMap<>();
        for (String partition : partitions) {
            String[] split = StringUtils.split(partition, "=");
            if (split.length == 2) {
                String s = split[1];
                List<String> pt = new ArrayList<>();
                for (String p : partitionTime) {
                    if (StringUtils.contains(p, s)) {
                        pt.add(p);
                    }
                }
                map.put(partition, pt);
            }
        }
        return map;
    }


}
