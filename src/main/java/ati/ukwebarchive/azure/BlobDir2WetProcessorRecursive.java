/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure;

import ati.ukwebarchive.data.CloudBlockMsg;
import ati.ukwebarchive.utils.Utils;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * EXPERIMENTAL. This class processes recursively all blobs (arc or warc) that belong to a
 * particular prefix in a storage container
 *
 * @author pierpaolo
 */
public class BlobDir2WetProcessorRecursive {

    /**
     * Store properties
     */
    private static Properties props;

    /**
     * Set of valid mime-types, only records with valid mime-type will be
     * processed
     */
    private static Set<String> validTypeSet;

    private static final Logger LOG = Logger.getLogger(BlobDir2WetProcessorRecursive.class.getName());

    private static CloudBlobContainer mainContainer;

    private static CloudBlobContainer storeContainer;

    private static ConcurrentLinkedQueue<CloudBlockMsg> queue;

    private static int c;

    private static void process(ListBlobItem item) {
        if (item instanceof CloudBlockBlob) {
            CloudBlockBlob block = (CloudBlockBlob) item;
            if (block.getName().endsWith(".arc.gz") || block.getName().endsWith(".warc.gz")) {
                if (queue.size() < 1000) {
                    queue.offer(new CloudBlockMsg(block, true));
                    c++;
                } else {
                    while (queue.size() >= 1000) {
                        try {
                            Thread.sleep(3 * 1000);
                        } catch (InterruptedException ex) {
                            Logger.getLogger(BlobDir2WetProcessorRecursive.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    queue.offer(new CloudBlockMsg(block, true));
                    c++;
                }
            }
        } else {
            LOG.log(Level.INFO, "Go into: {0}", item.getUri().toString());
            process(item);
        }
    }

    /**
     *
     * @param args The first argument is the prefix in the storage container
     */
    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            validTypeSet = Utils.loadFileInSet(new File(props.getProperty("contentTypeFilterFile")), true);
            final String uri = props.getProperty("uri");
            mainContainer = new CloudBlobContainer(new URI(uri));
            final String uriStore = props.getProperty("uriStore");
            storeContainer = new CloudBlobContainer(new URI(uriStore));
            storeContainer.createIfNotExists();
            File tmpDir = new File(props.getProperty("tempDir"));
            tmpDir.mkdirs();
            LOG.log(Level.INFO, "Processing block dir {0}", args[0]);
            queue = new ConcurrentLinkedQueue<>();
            int nt = Integer.parseInt(props.getProperty("mt.n"));
            List<Thread> lt = new ArrayList<>();
            for (int i = 0; i < nt; i++) {
                Blob2WetThread thread = new Blob2WetThread(queue, props, validTypeSet, storeContainer);
                lt.add(thread);
                thread.start();
            }
            c = 0;
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(args[0]);
            for (ListBlobItem itemBlock : listBlobs) {
                process(itemBlock);
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
            LOG.log(Level.INFO, "Processed blocks {0}", c);
        } catch (StorageException | IOException | URISyntaxException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }
}
