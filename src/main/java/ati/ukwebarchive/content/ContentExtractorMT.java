/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.content;

import ati.ukwebarchive.data.CloudBlockMsg;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import ati.ukwebarchive.utils.Utils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * This class extracts txt content from arc and warc blocks from Azure data storage using a multi-thread approach on a single VM 
 * @author pierpaolo
 */
public class ContentExtractorMT {

    /**
     * Store properties
     */
    public static Properties props;

    /**
     * Set of valid mime-types, only records with valid mime-type will be processed
     */
    public static Set<String> validTypeSet;

    private static Map<String, BufferedWriter> writerMap;

    private static int blockCount = 0;

    private static long parsingError = 0;

    private static long parsingOk = 0;

    /**
     * Add textual content to the corpus file. One corpus file is created for each YYYYMM time period
     * @param datekey The key used to retrieve the corpus file, this shuld be in the format YYYYMM (e.g. 199909)
     * @param content The textual content to store
     * @param header The header
     * @throws IOException
     */
    public synchronized static void addContent(String datekey, String content, String header) throws IOException {
        BufferedWriter writer = writerMap.get(datekey);
        if (writer == null) {
            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(props.getProperty("storePath") + "ukwebarchive_corpus_" + datekey + ".text.gz"))));
            writerMap.put(datekey, writer);
        }
        if (header == null) {
            header = "#--DOC START--UUID:" + UUID.randomUUID().toString();
        }
        writer.write(header);
        writer.newLine();
        writer.write(content);
        writer.newLine();
    }

    /**
     * Increase the number of processed blocks
     */
    public synchronized static void updateBlockCounter() {
        blockCount++;
        if (blockCount % 10 == 0) {
            System.out.print(".");
            if (blockCount % 1000 == 0) {
                System.out.println(blockCount);
            }
        }
    }

    /**
     * Increase the number of processed blocks with information about the number of processed records
     * @param error Records with errors
     * @param ok Records without errors
     */
    public synchronized static void updateBlockCounter(long error, long ok) {
        blockCount++;
        parsingError += error;
        parsingOk += ok;
        Logger.getLogger(ContentExtractorMT.class.getName()).log(Level.INFO, "PR:{0}\t{1}\t{2}", new Object[]{blockCount, parsingOk, parsingError});
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            validTypeSet = Utils.loadFileInSet(new File(props.getProperty("contentTypeFilterFile")), true);
            writerMap = new HashMap<>();
            File storeDir = new File(props.getProperty("storePath"));
            if (!storeDir.exists()) {
                storeDir.mkdirs();
            }
            File tmpDir = new File(props.getProperty("tempDir"));
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }
            final String uri = props.getProperty("uri");
            CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(uri));
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(props.getProperty("mainContainer"));
            int nt = Integer.parseInt(props.getProperty("mt.n"));
            ConcurrentLinkedQueue<CloudBlockMsg> queue = new ConcurrentLinkedQueue<>();
            int thfull = nt * 25;
            List<ContentThread> threads = new ArrayList<>();
            for (int i = 0; i < nt; i++) {
                threads.add(new ContentThread(queue));
                threads.get(i).start();
            }
            for (ListBlobItem item : listBlobs) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                    Iterable<ListBlobItem> listBlobs1 = cdir.listBlobs();
                    for (ListBlobItem itemBlock : listBlobs1) {
                        if (itemBlock instanceof CloudBlockBlob) {
                            CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                            if (block.getName().endsWith(".arc.gz") || block.getName().endsWith(".warc.gz")) {
                                if (queue.size() < thfull) {
                                    queue.offer(new CloudBlockMsg(block, true));
                                } else {
                                    try {
                                        while (queue.size() >= thfull) {
                                            Thread.sleep(10000);
                                        }
                                        queue.offer(new CloudBlockMsg(block, true));
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(ContentExtractorMT.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for (int i = 0; i < nt; i++) {
                queue.add(new CloudBlockMsg(null, false));
            }
            for (int i = 0; i < nt; i++) {
                try {
                    threads.get(i).join();
                } catch (InterruptedException ex) {
                    Logger.getLogger(ContentExtractorMT.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            for (BufferedWriter w : writerMap.values()) {
                w.close();
            }
            Logger.getLogger(ContentExtractorMT.class.getName()).log(Level.INFO, "END:{0}\t{1}\t{2}", new Object[]{blockCount, parsingOk, parsingError});
        } catch (IOException | URISyntaxException | StorageException ex) {
            Logger.getLogger(ContentExtractorMT.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
