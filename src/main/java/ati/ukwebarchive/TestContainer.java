/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive;

import static ati.ukwebarchive.azure.test.ClientTestTask.props;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 *
 * @author pierpaolo
 */
public class TestContainer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, URISyntaxException, StorageException {
        Properties props = new Properties();
        props.load(new FileReader("config.properties"));
        final String uri = props.getProperty("uri");
        CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(uri));
        Iterable<ListBlobItem> listBlobs = mainContainer.listBlobs(props.getProperty("mainContainer"));
        for (ListBlobItem item : listBlobs) {
            if (item instanceof CloudBlobDirectory) {
                CloudBlobDirectory cdir = (CloudBlobDirectory) item;
                Iterable<ListBlobItem> listBlobs1 = cdir.listBlobs();
                for (ListBlobItem itemBlock : listBlobs1) {
                    if (itemBlock instanceof CloudBlockBlob) {
                        CloudBlockBlob block = (CloudBlockBlob) itemBlock;
                        if (block.getName().endsWith(".arc.gz") || block.getName().endsWith(".warc.gz")) {
                            System.out.println(block.getName());
                        }
                    }
                }
            }
        }
    }
}
