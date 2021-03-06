/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure.tokenize;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This thread executes an upload of a file on a CloudBlobContainer
 * @author pierpaolo
 */
public class UploadThread extends Thread {

    private final CloudBlobContainer storeContainer;

    private final String blockReference;

    private final File file;

    /**
     * The class constructor
     * @param storeContainer The storage container
     * @param blockReference The reference to the cloud blob
     * @param file The file to upload
     */
    public UploadThread(CloudBlobContainer storeContainer, String blockReference, File file) {
        this.storeContainer = storeContainer;
        this.blockReference = blockReference;
        this.file = file;
    }

    @Override
    public void run() {
        try {
            CloudBlockBlob blockBlobReference = storeContainer.getBlockBlobReference(blockReference);
            blockBlobReference.uploadFromFile(file.getAbsolutePath());
        } catch (Exception ex) {
            Logger.getLogger(UploadThread.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            file.delete();
        }
    }

}
