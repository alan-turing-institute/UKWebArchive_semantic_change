/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure;

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
 *
 * @author pierpaolo
 */
public class ClientTaskMulti {

    /**
     * Store properties
     */
    private static Properties props;

    /**
     * Set of valid mime-types, only records with valid mime-type will be
     * processed
     */
    private static Set<String> validTypeSet;

    private static final Logger LOG = Logger.getLogger(ClientTaskMulti.class.getName());

    private static CloudBlobContainer mainContainer;

    private static CloudBlobContainer storeContainer;

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
            final ConcurrentLinkedQueue<BlockThreadObj> queue = new ConcurrentLinkedQueue<>();
            int nt = Integer.parseInt(props.getProperty("mt.n"));
            List<Thread> lt = new ArrayList<>();
            for (int i = 0; i < nt; i++) {
                ClientTaskMultiThread thread = new ClientTaskMultiThread(queue, props, validTypeSet, storeContainer);
                lt.add(thread);
                thread.start();
            }
            int c = 0;
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(args[0]);
            for (ListBlobItem itemBlock : listBlobs) {
                if (itemBlock instanceof CloudBlockBlob) {
                    CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                    if (block.getName().endsWith(".arc.gz") || block.getName().endsWith(".warc.gz")) {
                        if (queue.size() < 1000) {
                            queue.offer(new BlockThreadObj(block, true));
                            c++;
                        } else {
                            while (queue.size() >= 1000) {
                                try {
                                    Thread.sleep(3 * 1000);
                                } catch (InterruptedException ex) {
                                    Logger.getLogger(ClientTaskMulti.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                            queue.offer(new BlockThreadObj(block, true));
                            c++;
                        }
                    }
                }
            }
            for (int i = 0; i < nt; i++) {
                queue.offer(new BlockThreadObj(null, false));
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