/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure;

import ati.ukwebarchive.utils.Utils;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

/**
 *
 * @author pierpaolo
 */
public class ClientTaskMultiThread extends Thread {

    private final ConcurrentLinkedQueue<BlockThreadObj> queue;

    private final Properties props;

    private final Set<String> validTypeSet;

    private final CloudBlobContainer storeContainer;

    private boolean run = true;

    private static final Logger LOG = Logger.getLogger(ClientTaskMultiThread.class.getName());

    public ClientTaskMultiThread(ConcurrentLinkedQueue<BlockThreadObj> queue, Properties props, Set<String> validTypeSet, CloudBlobContainer storeContainer) {
        this.queue = queue;
        this.props = props;
        this.validTypeSet = validTypeSet;
        this.storeContainer = storeContainer;
    }

    @Override
    public void run() {
        while (run) {
            BlockThreadObj obj = queue.poll();
            if (obj != null) {
                if (obj.isValid()) {
                    CloudBlockBlob block = obj.getBlock();
                    String blockname = block.getName();
                    File tmpPath = null;
                    File wetFile = null;
                    if (blockname.endsWith(".arc.gz") || blockname.endsWith(".warc.gz")) {
                        LOG.log(Level.INFO, "Process block {0}.", blockname);
                        try {
                            int idxLastPathPart = block.getName().lastIndexOf("/");
                            if (idxLastPathPart > 0) {
                                String tmpFilename = block.getName().substring(idxLastPathPart + 1);
                                tmpPath = new File(props.getProperty("tempDir") + "/" + tmpFilename);
                                if (block.getName().endsWith(".arc.gz")) {
                                    wetFile = new File(props.getProperty("tempDir") + "/" + tmpFilename.replace(".arc.", ".wet."));
                                } else if (block.getName().endsWith(".warc.gz")) {
                                    wetFile = new File(props.getProperty("tempDir") + "/" + tmpFilename.replace(".warc.", ".wet."));
                                }
                                CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference(blockname.substring(0, idxLastPathPart) + "/" + wetFile.getName());
                                if (!blockBlobReference.exists()) {
                                    block.downloadToFile(tmpPath.getAbsolutePath());
                                    InputStream in = new GZIPInputStream(new FileInputStream(tmpPath));
                                    long error = 0;
                                    long ok = 0;
                                    if (block.getName().endsWith(".arc.gz")) {
                                        ArcReader reader = ArcReaderFactory.getReader(in);
                                        WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(wetFile), 8196);
                                        ArcRecordBase record;
                                        while ((record = reader.getNextRecord()) != null) {
                                            try {
                                                HttpHeader httpHeader = record.getHttpHeader();
                                                if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null && record.getArchiveDateStr() != null) {
                                                    if (httpHeader.statusCodeStr.equals("200") && validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                                        InputStream is = record.getPayloadContent();
                                                        if (is != null && is.available() > 0) {
                                                            try {
                                                                WarcRecord wetRecord = WarcRecord.createRecord(warcWriter);
                                                                String text = Utils.getContent(is);
                                                                text = text.replaceAll("\\n+", "\n");
                                                                if (text.length() > 0) {
                                                                    byte[] bytes = text.getBytes();
                                                                    wetRecord.header.warcDate = record.header.archiveDate;
                                                                    wetRecord.header.warcDateStr = record.header.archiveDateStr;
                                                                    wetRecord.header.warcTargetUriStr = record.header.urlStr;
                                                                    wetRecord.header.warcTargetUriUri = record.header.urlUri;
                                                                    wetRecord.header.contentLength = new Long(bytes.length);
                                                                    wetRecord.header.contentLengthStr = String.valueOf(bytes.length);
                                                                    wetRecord.header.contentType = record.header.contentType;
                                                                    wetRecord.header.contentTypeStr = record.header.contentTypeStr;
                                                                    InputStream wis = new ByteArrayInputStream(bytes);
                                                                    warcWriter.writeHeader(wetRecord);
                                                                    warcWriter.streamPayload(wis);
                                                                    ok++;
                                                                }
                                                            } catch (Exception | NoClassDefFoundError ex) {
                                                                error++;
                                                            }
                                                        }
                                                    }
                                                }
                                            } catch (Exception ex) {
                                                LOG.log(Level.WARNING, "Skip record in block: " + block.getName(), ex);
                                                error++;
                                            }
                                        }
                                        reader.close();
                                        warcWriter.close();
                                        blockBlobReference.uploadFromFile(wetFile.getAbsolutePath());
                                        wetFile.delete();
                                    } else if (block.getName().endsWith(".warc.gz")) {
                                        WarcReader reader = WarcReaderFactory.getReader(in);
                                        WarcRecord record;
                                        WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(wetFile), 8196);
                                        while ((record = reader.getNextRecord()) != null) {
                                            try {
                                                HttpHeader httpHeader = record.getHttpHeader();
                                                if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null && record.header.warcDateStr != null) {
                                                    if (httpHeader.statusCodeStr.equals("200") && validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                                        InputStream is = httpHeader.getPayloadInputStream();
                                                        if (is != null && is.available() > 0) {
                                                            try {
                                                                WarcRecord wetRecord = WarcRecord.createRecord(warcWriter);
                                                                String text = Utils.getContent(is);
                                                                text = text.replaceAll("\\n+", "\n");
                                                                if (text.length() > 0) {
                                                                    byte[] bytes = text.getBytes();
                                                                    wetRecord.header.warcDate = record.header.warcDate;
                                                                    wetRecord.header.warcDateStr = record.header.warcDateStr;
                                                                    wetRecord.header.warcTargetUriStr = record.header.warcTargetUriStr;
                                                                    wetRecord.header.warcTargetUriUri = record.header.warcTargetUriUri;
                                                                    wetRecord.header.contentLength = new Long(bytes.length);
                                                                    wetRecord.header.contentLengthStr = String.valueOf(bytes.length);
                                                                    wetRecord.header.contentType = record.header.contentType;
                                                                    wetRecord.header.contentTypeStr = record.header.contentTypeStr;
                                                                    InputStream wis = new ByteArrayInputStream(bytes);
                                                                    warcWriter.writeHeader(wetRecord);
                                                                    warcWriter.streamPayload(wis);
                                                                    ok++;
                                                                }
                                                            } catch (Exception | NoClassDefFoundError ex) {
                                                                error++;
                                                            }
                                                        }
                                                    }
                                                }
                                            } catch (Exception ex) {
                                                LOG.log(Level.WARNING, "Skip record in block: " + block.getName(), ex);
                                                error++;
                                            }
                                        }
                                        reader.close();
                                        warcWriter.close();
                                        blockBlobReference.uploadFromFile(wetFile.getAbsolutePath());
                                        wetFile.delete();
                                    }
                                    in.close();
                                    tmpPath.delete();
                                    LOG.log(Level.INFO, "Block {0}, ok {1}, error {2}", new Object[]{blockname, ok, error});
                                }
                            }
                        } catch (Exception ex) {
                            LOG.log(Level.WARNING, "Skip block: " + blockname, ex);
                            if (tmpPath != null) {
                                tmpPath.delete();
                            }
                            if (wetFile != null) {
                                wetFile.delete();
                            }
                        }
                        System.gc();
                    }
                } else {
                    run = false;
                }
            } else {
                try {
                    this.sleep(3 * 1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ClientTaskMultiThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    @Override
    public void interrupt() {
        this.run = false;
    }

}
