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
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pierpaolo
 */
public class ProgressStat {

    private static CloudBlobContainer mainContainer;

    private static CloudBlobContainer storeContainer;

    private static final Logger LOG = Logger.getLogger(ProgressStat.class.getName());

    private static Properties props;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            final String uri = props.getProperty("uri");
            mainContainer = new CloudBlobContainer(new URI(uri));
            final String uriStore = props.getProperty("uriStore");
            storeContainer = new CloudBlobContainer(new URI(uriStore));
            Map<String, Integer> warcMap = new HashMap<>();
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(props.getProperty("mainContainer"));
            for (ListBlobItem item : listBlobs) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                    Iterable<ListBlobItem> listBlobs1 = cdir.listBlobs();
                    for (ListBlobItem itemBlock : listBlobs1) {
                        if (itemBlock instanceof CloudBlockBlob) {
                            CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                            if (block.getName().endsWith(".arc.gz") || block.getName().endsWith(".warc.gz")) {
                                Integer c = warcMap.get(cdir.getPrefix());
                                if (c == null) {
                                    warcMap.put(cdir.getPrefix(), 1);
                                } else {
                                    warcMap.put(cdir.getPrefix(), c + 1);
                                }
                            }
                        }
                    }
                    System.out.print(".");
                }
            }
            System.out.println();
            Map<String, Integer> wetMap = new HashMap<>();
            listBlobs = storeContainer.listBlobs(props.getProperty("mainContainer"));
            for (ListBlobItem item : listBlobs) {
                if (item instanceof CloudBlobDirectory) {
                    CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                    Iterable<ListBlobItem> listBlobs1 = cdir.listBlobs();
                    for (ListBlobItem itemBlock : listBlobs1) {
                        if (itemBlock instanceof CloudBlockBlob) {
                            CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                            if (block.getName().endsWith(".wet.gz")) {
                                Integer c = wetMap.get(cdir.getPrefix());
                                if (c == null) {
                                    wetMap.put(cdir.getPrefix(), 1);
                                } else {
                                    wetMap.put(cdir.getPrefix(), c + 1);
                                }
                            }
                        }
                    }
                    System.out.print(".");
                }
            }
            System.out.println();
            //stat
            for (Map.Entry<String, Integer> e : warcMap.entrySet()) {
                Integer wetc = wetMap.get(e.getKey());
                if (wetc == null) {
                    System.out.println(e.getKey() + "\t" + e.getValue() + "\t0\t0");
                } else {
                    System.out.println(e.getKey() + "\t" + e.getValue() + "\t" + wetc.intValue() + "\t" + (wetc.doubleValue() / e.getValue().doubleValue()));
                }
            }
        } catch (IOException | URISyntaxException | StorageException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

}
