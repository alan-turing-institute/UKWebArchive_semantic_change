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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class shows the WET files in the store container
 * @author pierpaolo
 */
public class ProgressStatWet {

    private static CloudBlobContainer storeContainer;

    private static final Logger LOG = Logger.getLogger(ProgressStatWet.class.getName());

    private static Properties props;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            final String uriStore = props.getProperty("uriStore");
            storeContainer = new CloudBlobContainer(new URI(uriStore));
            Map<String, Integer> wetMap = new HashMap<>();
            Iterable<ListBlobItem> listBlobs = storeContainer.listBlobs(props.getProperty("mainContainer"));
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
            List<String> keys = new ArrayList<>(wetMap.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                Integer wetc = wetMap.get(key);
                System.out.println(key + "\t" + wetc.intValue());
            }
        } catch (IOException | URISyntaxException | StorageException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

}
