/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.utils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class uploads GZip files from a local directory to a blobs container
 * @author pierpaolo
 */
public class UploadGZipFiles {

    private static final Logger LOG = Logger.getLogger(UploadGZipFiles.class.getName());

    /**
     * args[0]=blob container URI, args[1]=blob container prefix, args[2]=local directory
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length == 3) {
            try {
                CloudBlobContainer mainContainer = new CloudBlobContainer(new URI(args[0]));
                LOG.log(Level.INFO, "Container prefix: {0}", args[1]);
                File occdir = new File(args[2]);
                LOG.log(Level.INFO, "Occ. matrix dir: {0}", occdir.getAbsolutePath());
                int n = 0;
                long l = 0;
                File[] listFiles = occdir.listFiles();
                for (File file : listFiles) {
                    if (file.isFile() && file.getName().endsWith(".gz")) {
                        CloudBlockBlob ref = mainContainer.getBlockBlobReference(args[1] + "/" + file.getName());
                        LOG.log(Level.INFO, "Upload: {0}", file.getName());
                        ref.uploadFromFile(file.getAbsolutePath());
                        l += file.length();
                        n++;
                    }
                }
                LOG.log(Level.INFO, "Upload {0} files.", n);
                LOG.log(Level.INFO, "Total bytes {0}", l);
            } catch (URISyntaxException | StorageException ex) {
                LOG.log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(UploadGZipFiles.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
