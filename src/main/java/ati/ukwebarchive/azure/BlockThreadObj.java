/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.azure;

import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 *
 * @author pierpaolo
 */
public class BlockThreadObj {
    
    private final CloudBlockBlob block;
    
    private final boolean valid;

    public BlockThreadObj(CloudBlockBlob block, boolean valid) {
        this.block = block;
        this.valid = valid;
    }

    public CloudBlockBlob getBlock() {
        return block;
    }

    public boolean isValid() {
        return valid;
    }
    
    
    
}
