/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.reddison;

import ati.ukwebarchive.content.ContentExtractorMT;
import ati.ukwebarchive.data.CloudBlockMsg;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

/**
 *
 * @author pierpaolo
 */
public class Master {

    /**
     * Store properties
     */
    public static Properties props;

    private static RedissonClient redisson;

    public static void main(String[] args) {
        try {
            props = new Properties();
            props.load(new FileReader("config.properties"));
            Config config = new Config();
            ClusterServersConfig csConfig = config.useClusterServers().setScanInterval(3000);
            String[] addresses = props.getProperty("redisson.clusterNodes").split(";");
            csConfig.addNodeAddress(addresses);
            redisson = Redisson.create(config);
            int qload = Integer.parseInt(props.getProperty("redisson.queueLoad"));
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
                                RBlockingQueue<CloudBlockMsg> queue = redisson.getBlockingQueue("cloudBlockQueue");
                                if (queue.size() < qload) {
                                    queue.offer(new CloudBlockMsg(block, true));
                                } else {
                                    try {
                                        while (queue.size() >= qload) {
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
            int numClients = Integer.parseInt(props.getProperty("numClients"));
            RBlockingQueue<CloudBlockMsg> queue = redisson.getBlockingQueue("cloudBlockQueue");
            for (int i = 0; i < numClients; i++) {
                queue.add(new CloudBlockMsg(null, false));
            }
        } catch (IOException | URISyntaxException | StorageException ex) {
            Logger.getLogger(ContentExtractorMT.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
