/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure.test;

import ati.ukwebarchive.content.ContentThread;
import ati.ukwebarchive.utils.Utils;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Set;
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

/**
 *
 * @author pierpaolo
 */
public class ClientTestTask {

    /**
     * Store properties
     */
    public static Properties props;

    /**
     * Set of valid mime-types, only records with valid mime-type will be
     * processed
     */
    public static Set<String> validTypeSet;

    private static final Logger LOG = Logger.getLogger(ClientTestTask.class.getName());

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
            LOG.log(Level.INFO, "Process block {0}.", blockname);
            try {
                CloudBlockBlob block = mainContainer.getBlockBlobReference(blockname);
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
                        BufferedWriter writer = new BufferedWriter(new FileWriter(props.getProperty("tempDir") + "/" + tmpFilename.replace(".arc.gz", ".stat.tsv")));
                        ArcRecordBase record;
                        while ((record = reader.getNextRecord()) != null) {
                            try {
                                HttpHeader httpHeader = record.getHttpHeader();
                                if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null && record.getArchiveDateStr() != null) {
                                    if (httpHeader.statusCodeStr.equals("200") && validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                        InputStream is = record.getPayloadContent();
                                        if (is != null && is.available() > 0) {
                                            try {
                                                writer.append(record.header.archiveDateStr).append("\t");
                                                writer.append(record.header.urlStr).append("\t");
                                                writer.append(record.header.contentTypeStr).append("\t");
                                                writer.append(record.header.archiveLengthStr);
                                                writer.newLine();
                                                ok++;
                                            } catch (IOException ex) {
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
                        writer.close();
                        File statFile = new File(props.getProperty("tempDir") + "/" + tmpFilename.replace(".arc.gz", ".stat.tsv"));
                        CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference(block.getName().substring(0, idxLastPathPart) + "/" + statFile.getName());
                        blockBlobReference.uploadFromFile(statFile.getAbsolutePath());
                        statFile.delete();
                    } else if (block.getName().endsWith(".warc.gz")) {
                        WarcReader reader = WarcReaderFactory.getReader(in);
                        WarcRecord record;
                        BufferedWriter writer = new BufferedWriter(new FileWriter(props.getProperty("tempDir") + "/" + tmpFilename.replace(".warc.gz", ".stat.tsv")));
                        while ((record = reader.getNextRecord()) != null) {
                            try {
                                HttpHeader httpHeader = record.getHttpHeader();
                                if (httpHeader != null && httpHeader.contentType != null && httpHeader.statusCodeStr != null && record.header.warcDateStr != null) {
                                    if (httpHeader.statusCodeStr.equals("200") && validTypeSet.contains(Utils.getBaseContentType(httpHeader.contentType))) {
                                        InputStream is = httpHeader.getPayloadInputStream();
                                        if (is != null && is.available() > 0) {
                                            try {
                                                writer.append(record.header.warcDateStr).append("\t");
                                                writer.append(record.header.warcTargetUriStr).append("\t");
                                                writer.append(record.header.contentTypeStr).append("\t");
                                                writer.append(record.header.contentLengthStr);
                                                writer.newLine();
                                                ok++;
                                            } catch (IOException ex) {
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
                        writer.close();
                        File statFile = new File(props.getProperty("tempDir") + "/" + tmpFilename.replace(".warc.gz", ".stat.tsv"));
                        CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference(blockname.substring(0, idxLastPathPart) + "/" + statFile.getName());
                        blockBlobReference.uploadFromFile(statFile.getAbsolutePath());
                        statFile.delete();
                    }
                    in.close();
                    tmpPath.delete();
                }
            } catch (Exception ex) {
                Logger.getLogger(ContentThread.class.getName()).log(Level.WARNING, "Skip block: " + blockname, ex);
            }
        } catch (StorageException | IOException | URISyntaxException ex) {
            Logger.getLogger(ClientTestTask.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
