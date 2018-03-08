/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.content;

import ati.ukwebarchive.data.CloudBlockMsg;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import ati.ukwebarchive.utils.Utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
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

/**
 * This class is a single thread that processes blocks from the Data Storage.
 * Information about blocks are retrieved from a shared queue
 * @author pierpaolo
 */
public class ContentThread extends Thread {

    private final ConcurrentLinkedQueue<CloudBlockMsg> queue;

    private boolean run = true;

    /**
     * Thread constructor with the shared queue
     * @param queue
     */
    public ContentThread(ConcurrentLinkedQueue<CloudBlockMsg> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (run) {
            CloudBlockMsg msg = queue.poll();
            if (msg != null) {
                if (msg.isValid()) {
                    CloudBlockBlob block = msg.getBlock();
                    try {
                        int idxLastPathPart = block.getName().lastIndexOf("/");
                        if (idxLastPathPart > 0) {
                            String tmpFilename = block.getName().substring(idxLastPathPart + 1);
                            File tmpPath = new File(ContentExtractorMT.props.getProperty("tempDir") + "/" + tmpFilename);
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
                                            if (httpHeader.statusCodeStr.equals("200") && ContentExtractorMT.validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                                String datekey = Utils.getYearMonthFromDateStr(record.getArchiveDateStr());
                                                InputStream is = record.getPayloadContent();
                                                if (is != null) {
                                                    try {
                                                        String content = Utils.getContent(is);
                                                        String header = "#--DOC START--URL:" + record.getUrlStr();
                                                        ContentExtractorMT.addContent(datekey, content, header);
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
                                            if (httpHeader.statusCodeStr.equals("200") && ContentExtractorMT.validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                                String datekey = Utils.getYearMonthFromDateStr(record.header.warcDateStr.replace("-", ""));
                                                InputStream is = httpHeader.getPayloadInputStream();
                                                if (is != null) {
                                                    try {
                                                        String content = Utils.getContent(is);
                                                        String header = "#--DOC START--URL:" + record.header.warcTargetUriStr;
                                                        ContentExtractorMT.addContent(datekey, content, header);
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
                            ContentExtractorMT.updateBlockCounter(error, ok);
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip block: " + block.getName(), ex);
                    }
                } else {
                    run = false;
                }
            }
        }
    }

    @Override
    public void interrupt() {
        this.run = false;
        super.interrupt();
    }

}
