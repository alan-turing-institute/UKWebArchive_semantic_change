/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.FileReader;
import java.net.URI;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class computes the number of WET blobs that belong to a specific prefix into the storage container
 * @author pierpaolo
 */
public class CheckNumberWet {

    /**
     * @param args the command line arguments, args[0] is the prefix of the storage container
     */
    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.load(new FileReader("config.properties"));
            final String uriStore = props.getProperty("uriStore");
            CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(uriStore));
            Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(args[0]);
            int c = 0;
            System.out.println();
            for (ListBlobItem itemBlock : listBlobs) {
                if (itemBlock instanceof CloudBlockBlob) {
                    CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                    if (block.getName().endsWith(".wet.gz")) {
                        c++;
                        if (c % 100 == 0) {
                            System.out.print(".");
                            if (c % 10000 == 0) {
                                System.out.println(c);
                            }
                        }
                    }
                }
            }
            System.out.println(c);
        } catch (Exception ex) {
            Logger.getLogger(CheckNumberWet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
