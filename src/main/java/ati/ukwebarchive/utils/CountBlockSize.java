/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class recursively counts all blocks starting from the URI and the prefix
 * in the cloud storage. Blocks are filtered by the name extension.
 *
 * The class
 *
 * @author pierpaolo
 */
public class CountBlockSize {

    private static final Logger LOG = Logger.getLogger(CountBlockSize.class.getName());

    private static long nb = 0;

    private static long lb = 0;

    private static String fileExt = ".gz";

    private static Map<String, Integer> prefixCount = new HashMap<>();

    private static Map<String, Long> prefixLength = new HashMap<>();

    private static void process(ListBlobItem item) throws StorageException, URISyntaxException {
        if (item instanceof CloudBlockBlob) {
            CloudBlockBlob blob = (CloudBlockBlob) item;
            if (blob.getName().endsWith(fileExt)) {
                int idx = blob.getName().lastIndexOf("/");
                String prefix = blob.getName().substring(0, idx);
                Integer c = prefixCount.get(prefix);
                if (c == null) {
                    prefixCount.put(prefix, 1);
                } else {
                    prefixCount.put(prefix, c + 1);
                }
                Long l = prefixLength.get(prefix);
                if (l == null) {
                    prefixLength.put(prefix, blob.getProperties().getLength());
                } else {
                    prefixLength.put(prefix, l + blob.getProperties().getLength());
                }
                nb++;
                if (nb % 100 == 0) {
                    System.out.print(".");
                    if (nb % 10000 == 0) {
                        System.out.println(nb);
                    }
                }
                lb += blob.getProperties().getLength();
            }
        } else if (item instanceof CloudBlobDirectory) {
            CloudBlobDirectory dir = (CloudBlobDirectory) item;
            Iterable<ListBlobItem> listBlobs = dir.listBlobs();
            for (ListBlobItem initem : listBlobs) {
                process(initem);
            }
        }
    }

    /**
     * @param args the command line arguments args[0] is the URI, args[1] is the
     * start prefix, args[2] is the filename extension
     */
    public static void main(String[] args) {
        try {
            CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(args[0]));
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(args[1]);
            fileExt = args[2];
            System.out.println();
            for (ListBlobItem itemBlock : listBlobs) {
                process(itemBlock);
            }
            System.out.println();
            double mbyte = (double) lb / 1024 / 1024;
            double gbyte = (double) lb / 1024 / 1024 / 1024;
            double tbyte = (double) lb / 1024 / 1024 / 1024 / 1024;
            LOG.log(Level.INFO, "Number of blocks: {0}", nb);
            LOG.log(Level.INFO, "Total bytes: {0}", lb);
            LOG.log(Level.INFO, "Mbytes: {0}", mbyte);
            LOG.log(Level.INFO, "Gbytes: {0}", gbyte);
            LOG.log(Level.INFO, "Tbytes: {0}", tbyte);
            List<String> keys = new ArrayList<>(prefixCount.keySet());
            Collections.sort(keys);
            System.out.println();
            for (String key : keys) {
                System.out.println(key + "\t" + prefixCount.get(key) + "\t" + prefixLength.get(key));
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

}
