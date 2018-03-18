/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure;

import ati.ukwebarchive.content.ContentThread;
import ati.ukwebarchive.utils.Utils;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

/**
 *
 * @author pierpaolo
 */
public class ClientTask {

    /**
     * Store properties
     */
    public static Properties props;

    /**
     * Set of valid mime-types, only records with valid mime-type will be
     * processed
     */
    public static Set<String> validTypeSet;

    private static final Logger LOG = Logger.getLogger(ClientTask.class.getName());

    private static CloudBlobContainer mainContainer;

    private static CloudBlobContainer storeContainer;

    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            LOG.log(Level.INFO, "Create queue client.");
            validTypeSet = Utils.loadFileInSet(new File(props.getProperty("contentTypeFilterFile")), true);
            final String uri = props.getProperty("uri");
            mainContainer = new CloudBlobContainer(new URI(uri));
            final String uriStore = props.getProperty("uriStore");
            storeContainer = new CloudBlobContainer(new URI(uriStore));
            storeContainer.createIfNotExists();
            File tmpDir = new File(props.getProperty("tempDir"));
            tmpDir.mkdirs();
            String blockname = args[0];
            File tmpPath=null;
            LOG.log(Level.INFO, "Process block {0}.", blockname);
            try {
                CloudBlockBlob block = mainContainer.getBlockBlobReference(blockname);
                int idxLastPathPart = block.getName().lastIndexOf("/");
                if (idxLastPathPart > 0) {
                    String tmpFilename = block.getName().substring(idxLastPathPart + 1);
                    tmpPath = new File(props.getProperty("tempDir") + "/" + tmpFilename);
                    block.downloadToFile(tmpPath.getAbsolutePath());
                    InputStream in = new GZIPInputStream(new FileInputStream(tmpPath));
                    long error = 0;
                    long ok = 0;
                    if (block.getName().endsWith(".arc.gz")) {
                        ArcReader reader = ArcReaderFactory.getReader(in);
                        WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(props.getProperty("tempDir") + "/" + tmpFilename.replace(".arc.", ".wet.")), 8196);
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
                                            } catch (IOException | TikaException ex) {
                                                error++;
                                            }
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip record in block: " + block.getName(), ex);
                                error++;
                            }
                        }
                        reader.close();
                        warcWriter.close();
                        File wetFile = new File(props.getProperty("tempDir") + "/" + tmpFilename.replace(".arc.", ".wet."));
                        CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference(blockname.substring(0, idxLastPathPart) + "/" + wetFile.getName());
                        blockBlobReference.uploadFromFile(wetFile.getAbsolutePath());
                        wetFile.delete();
                    } else if (block.getName().endsWith(".warc.gz")) {
                        WarcReader reader = WarcReaderFactory.getReader(in);
                        WarcRecord record;
                        WarcWriter warcWriter = WarcWriterFactory.getWriterCompressed(new FileOutputStream(props.getProperty("tempDir") + "/" + tmpFilename.replace(".warc.", ".wet.")), 8196);
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
                                            } catch (IOException | TikaException ex) {
                                                error++;
                                            }
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip record in block: " + block.getName(), ex);
                                error++;
                            }
                        }
                        reader.close();
                        warcWriter.close();
                        File wetFile = new File(props.getProperty("tempDir") + "/" + tmpFilename.replace(".warc.", ".wet."));
                        CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference(blockname.substring(0, idxLastPathPart) + "/" + wetFile.getName());
                        blockBlobReference.uploadFromFile(wetFile.getAbsolutePath());
                        wetFile.delete();
                    }
                    in.close();
                    tmpPath.delete();
                    /*if (ok % 1000 == 0) {
                        Utils.newTika();
                    }*/
                }
            } catch (Exception ex) {
                Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip block: " + blockname, ex);
                if (tmpPath!=null) {
                    tmpPath.delete();
                }
            }
        } catch (StorageException | IOException | URISyntaxException ex) {
            Logger.getLogger(ClientTask.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
