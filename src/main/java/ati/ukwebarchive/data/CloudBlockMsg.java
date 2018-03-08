/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.data;

import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.util.Objects;

/**
 * This massage holds the reference to the Azure cloud block
 *
 * @author pierpaolo
 */
public class CloudBlockMsg {

    private final CloudBlockBlob block;

    private final boolean valid;

    /**
     *
     * @param block
     * @param valid
     */
    public CloudBlockMsg(CloudBlockBlob block, boolean valid) {
        this.block = block;
        this.valid = valid;
    }

    /**
     *
     * @param block
     */
    public CloudBlockMsg(CloudBlockBlob block) {
        this(block, true);
    }

    /**
     *
     * @return
     */
    public CloudBlockBlob getBlock() {
        return block;
    }

    /**
     *
     * @return
     */
    public boolean isValid() {
        return valid;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + Objects.hashCode(this.block);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CloudBlockMsg other = (CloudBlockMsg) obj;
        if (!Objects.equals(this.block, other.block)) {
            return false;
        }
        return true;
    }

}
