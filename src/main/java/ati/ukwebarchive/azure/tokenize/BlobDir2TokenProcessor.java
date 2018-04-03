/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure.tokenize;

import ati.ukwebarchive.data.CloudBlockMsg;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * This class processes (tokenize) all blobs (wet) that belong to a particular
 * prefix in a storage container
 *
 * @author pierpaolo
 */
public class BlobDir2TokenProcessor {

    /**
     * Store properties
     */
    private static Properties props;

    private static final Logger LOG = Logger.getLogger(BlobDir2TokenProcessor.class.getName());

    private static CloudBlobContainer storeContainer;

    private static Map<String, WriterInfo> map = new HashMap<>();

    private static long BLOCK_MAX_SIZE = 256 * 1024 * 1024; //512 Mbyte

    private static String uuid = UUID.randomUUID().toString().replace("-", "");

    private static List<Thread> uploadThread = new ArrayList<>();

    private static FileWriter logWriter;

    /**
     *
     * @param key
     * @param tokens
     * @throws IOException
     */
    public static synchronized void write(String key, List<String> tokens) throws IOException {
        if (!key.matches("^[1-2][0-9][0-9][0-9](00|01|02|03|04|05|06|07|08|09|10|11|12)$")) {
            return;
        }
        WriterInfo winfo = map.get(key);
        if (winfo == null) {
            winfo = new WriterInfo();
            winfo.setFile(new File(props.getProperty("tempDir") + "/ukwac-tk-" + uuid + "-0-" + key + ".gz"));
            winfo.setWriter(new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(winfo.getFile())))));
            map.put(key, winfo);
        } else {
            long length = winfo.getFile().length();
            if (length > BLOCK_MAX_SIZE) {
                winfo.getWriter().close();
                Thread t = new UploadThread(storeContainer, "ukwac-tk/D-" + key + "/ukwac-tk-" + key + "-" + winfo.getFile().getName(), new File(winfo.getFile().getAbsolutePath()));
                t.start();
                uploadThread.add(t);
                winfo.setCounter(winfo.getCounter() + 1);
                winfo.setFile(new File(props.getProperty("tempDir") + "/ukwac-tk-" + uuid + "-" + winfo.getCounter() + "-" + key + ".gz"));
                winfo.setWriter(new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(winfo.getFile())))));
                map.put(key, winfo);
            }
        }
        BufferedWriter writer = winfo.getWriter();
        for (String token : tokens) {
            writer.append(token).append(" ");
        }
        writer.newLine();
    }

    /**
     *
     * @param message
     * @throws IOException
     */
    public static synchronized void logMessage(String message) throws IOException {
        logWriter.write(message);
        logWriter.write("\n");
        logWriter.flush();
    }

    /**
     *
     * @param args args[0] is the prefix in the storage container
     */
    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            final String uriStore = props.getProperty("uriStore");
            storeContainer = new CloudBlobContainer(new URI(uriStore));
            storeContainer.createIfNotExists();
            File tmpDir = new File(props.getProperty("tempDir"));
            tmpDir.mkdirs();
            logWriter = new FileWriter("ukwac-tk-" + uuid + ".log");
            logMessage("UUID: "+uuid);
            LOG.log(Level.INFO, "Processing block dir {0}", args[0]);
            final ConcurrentLinkedQueue<CloudBlockMsg> queue = new ConcurrentLinkedQueue<>();
            int nt = Integer.parseInt(props.getProperty("mt.n"));
            List<Thread> lt = new ArrayList<>();
            for (int i = 0; i < nt; i++) {
                Blob2TokenThread thread = new Blob2TokenThread(queue, props);
                lt.add(thread);
                thread.start();
            }
            int c = 0;
            Iterable<ListBlobItem> listBlobs = storeContainer.listBlobs(args[0]);
            for (ListBlobItem itemBlock : listBlobs) {
                if (itemBlock instanceof CloudBlockBlob) {
                    CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                    if (block.getName().endsWith(".wet.gz")) {
                        if (queue.size() < 1000) {
                            queue.offer(new CloudBlockMsg(block, true));
                            c++;
                        } else {
                            while (queue.size() >= 1000) {
                                try {
                                    //clean dead upload thread
                                    for (int k = uploadThread.size() - 1; k >= 0; k--) {
                                        if (!uploadThread.get(k).isAlive()) {
                                            uploadThread.remove(k);
                                        }
                                    }
                                    Thread.sleep(3 * 10);
                                } catch (InterruptedException ex) {
                                    Logger.getLogger(BlobDir2TokenProcessor.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                            queue.offer(new CloudBlockMsg(block, true));
                            c++;
                        }
                    }
                }
            }
            for (int i = 0; i < nt; i++) {
                queue.offer(new CloudBlockMsg(null, false));
            }
            LOG.info("Wait for threads...");
            for (int i = 0; i < nt; i++) {
                try {
                    lt.get(i).join();
                } catch (InterruptedException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
            LOG.info("Wait for uploads...");
            for (int i = 0; i < uploadThread.size(); i++) {
                try {
                    uploadThread.get(i).join();
                } catch (InterruptedException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
            LOG.info("Upload last files...");
            for (Map.Entry<String, WriterInfo> entry : map.entrySet()) {
                entry.getValue().getWriter().close();
                Thread t = new UploadThread(storeContainer, "ukwac-tk/D-" + entry.getKey() + "/ukwac-tk-" + entry.getKey() + "-" + entry.getValue().getFile().getName(), new File(entry.getValue().getFile().getAbsolutePath()));
                t.start();
                uploadThread.add(t);
            }
            LOG.info("Wait for last uploads...");
            for (int i = 0; i < uploadThread.size(); i++) {
                try {
                    uploadThread.get(i).join();
                } catch (InterruptedException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
            logWriter.write("Processed blocks: " + c);
            LOG.log(Level.INFO, "Processed blocks {0}", c);
            logWriter.close();
            LOG.info("Upload log...");
            CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference("ukwac-tk/logs/ukwac-tk-" + uuid + ".log");
            blockBlobReference.uploadFromFile("ukwac-tk-" + uuid + ".log");
            new File("ukwac-tk-" + uuid + ".log").delete();
        } catch (StorageException | IOException | URISyntaxException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    private static class WriterInfo {

        private File file;

        private BufferedWriter writer;

        private int counter = 0;

        public WriterInfo() {
        }

        public WriterInfo(File file) {
            this.file = file;
        }

        public int getCounter() {
            return counter;
        }

        public void setCounter(int counter) {
            this.counter = counter;
        }

        public File getFile() {
            return file;
        }

        public void setFile(File file) {
            this.file = file;
        }

        public BufferedWriter getWriter() {
            return writer;
        }

        public void setWriter(BufferedWriter writer) {
            this.writer = writer;
        }

    }
}
