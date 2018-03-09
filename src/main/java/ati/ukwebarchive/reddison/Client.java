/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.reddison;

import ati.ukwebarchive.content.ContentThread;
import ati.ukwebarchive.data.CloudBlockMsg;
import ati.ukwebarchive.utils.Utils;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.tika.exception.TikaException;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.redisson.Redisson;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

/**
 *
 * @author pierpaolo
 */
public class Client {

    /**
     * Store properties
     */
    public static Properties props;

    /**
     * Set of valid mime-types, only records with valid mime-type will be
     * processed
     */
    public static Set<String> validTypeSet;

    private static RedissonClient redisson;

    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            validTypeSet = Utils.loadFileInSet(new File(props.getProperty("contentTypeFilterFile")), true);
            Config config = new Config();
            ClusterServersConfig csConfig = config.useClusterServers().setScanInterval(3000);
            String[] addresses = props.getProperty("redisson.clusterNodes").split(";");
            csConfig.addNodeAddress(addresses);
            redisson = Redisson.create(config);
            boolean run = true;
            while (run) {
                RBlockingQueue<CloudBlockMsg> queue = redisson.getBlockingQueue("cloudBlockQueue");
                CloudBlockMsg msg = queue.poll();
                if (msg != null) {
                    if (msg.isValid()) {
                        CloudBlockBlob block = msg.getBlock();
                        try {
                            int idxLastPathPart = block.getName().lastIndexOf("/");
                            if (idxLastPathPart > 0) {
                                String tmpFilename = block.getName().substring(idxLastPathPart + 1);
                                File tmpPath = new File(props.getProperty("tempDir") + "/" + tmpFilename);
                                block.downloadToFile(tmpPath.getAbsolutePath());
                                InputStream in = new GZIPInputStream(new FileInputStream(tmpPath));
                                long error = 0;
                                long ok = 0;
                                if (block.getName().endsWith(".arc.gz")) {
                                    ArcReader reader = ArcReaderFactory.getReader(in);
                                    ArcRecordBase record;
                                    while ((record = reader.getNextRecord()) != null) {
                                        try {
                                            HttpHeader httpHeader = record.getHttpHeader();
                                            if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null && record.getArchiveDateStr() != null) {
                                                if (httpHeader.statusCodeStr.equals("200") && validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                                    String datekey = Utils.getYearMonthFromDateStr(record.getArchiveDateStr());
                                                    InputStream is = record.getPayloadContent();
                                                    if (is != null) {
                                                        try {
                                                            String content = Utils.getContent(is);
                                                            String header = "#--DOC START--URL:" + record.getUrlStr();
                                                            //TO DO store content in data storage
                                                            ok++;
                                                        } catch (IOException | TikaException ex) {
                                                            error++;
                                                        }
                                                    }
                                                }
                                            }
                                            record.close();
                                        } catch (Exception ex) {
                                            Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip record in block: " + block.getName(), ex);
                                            error++;
                                        }
                                    }
                                    reader.close();
                                } else if (block.getName().endsWith(".warc.gz")) {
                                    WarcReader reader = WarcReaderFactory.getReader(in);
                                    WarcRecord record;
                                    while ((record = reader.getNextRecord()) != null) {
                                        try {
                                            HttpHeader httpHeader = record.getHttpHeader();
                                            if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null && record.header.warcDateStr != null) {
                                                if (httpHeader.statusCodeStr.equals("200") && validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                                    String datekey = Utils.getYearMonthFromDateStr(record.header.warcDateStr.replace("-", ""));
                                                    InputStream is = httpHeader.getPayloadInputStream();
                                                    if (is != null) {
                                                        try {
                                                            String content = Utils.getContent(is);
                                                            String header = "#--DOC START--URL:" + record.header.warcTargetUriStr;
                                                            //TO DO store content in data storage
                                                            ok++;
                                                        } catch (IOException | TikaException ex) {
                                                            error++;
                                                        }
                                                    }
                                                }
                                            }
                                            record.close();
                                        } catch (Exception ex) {
                                            Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip record in block: " + block.getName(), ex);
                                            error++;
                                        }
                                    }
                                    reader.close();
                                }
                                in.close();
                                tmpPath.delete();
                                //TO DO update counter somewhere!??!
                            }
                        } catch (Exception ex) {
                            Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip block: " + block.getName(), ex);
                        }
                    } else {
                        run = false;
                    }
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
