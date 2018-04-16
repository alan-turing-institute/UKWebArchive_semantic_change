/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class download files from a blobs container to a local directory
 *
 * @author pierpaolo
 */
public class DownloadFiles {

    private static final Logger LOG = Logger.getLogger(DownloadFiles.class.getName());

    /**
     * args[0]=blob container URI, args[1]=blob container prefix, args[2]=local
     * directory, args[3]=file extension, args[4]=overwrite existing files
     * (true/false)
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length == 5) {
            try {
                CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(args[0]));
                LOG.log(Level.INFO, "Container prefix: {0}", args[1]);
                File localDir = new File(args[2]);
                LOG.log(Level.INFO, "Local directory: {0}", localDir.getAbsolutePath());
                boolean overwrite = Boolean.parseBoolean(args[4]);
                LOG.log(Level.INFO, "Overwrite existing files: {0}", overwrite);
                int n = 0;
                long l = 0;
                Iterable<ListBlobItem> blobs = mainContainer.listBlobs(args[1]);
                for (ListBlobItem itemBlob : blobs) {
                    if (itemBlob instanceof CloudBlockBlob) {
                        CloudBlockBlob blob = (CloudBlockBlob) itemBlob;
                        if (blob.getName().endsWith(args[3])) {
                            String blobLastName = blob.getName().substring(blob.getName().lastIndexOf("/"));
                            File outFile = new File(localDir.getAbsolutePath() + blobLastName);
                            if (overwrite || !outFile.exists()) {
                                LOG.log(Level.INFO, "Download blob: {0}", blob.getName());
                                blob.downloadToFile(outFile.getAbsolutePath());
                                l += blob.getProperties().getLength();
                                n++;
                            }
                        }
                    }
                }
                LOG.log(Level.INFO, "Download {0} files.", n);
                LOG.log(Level.INFO, "Total bytes {0}", l);
            } catch (URISyntaxException | StorageException ex) {
                LOG.log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(DownloadFiles.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
