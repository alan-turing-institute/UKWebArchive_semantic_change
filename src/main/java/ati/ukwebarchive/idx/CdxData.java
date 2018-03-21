/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ati.ukwebarchive.idx;

import java.util.Objects;

/**
 * An entry of a CDX file
 * @author pierpaolo
 */
public class CdxData {
    
    private String name;
    
    private String url;
    
    private String mineType;
    
    private String httpResponse;
    
    private long offset;
    
    private String arcfile;
    
    private String dateString;

    /**
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     */
    public String getUrl() {
        return url;
    }

    /**
     *
     * @param url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     *
     * @return
     */
    public String getMineType() {
        return mineType;
    }

    /**
     *
     * @param mineType
     */
    public void setMineType(String mineType) {
        this.mineType = mineType;
    }

    /**
     *
     * @return
     */
    public String getHttpResponse() {
        return httpResponse;
    }

    /**
     *
     * @param httpResponse
     */
    public void setHttpResponse(String httpResponse) {
        this.httpResponse = httpResponse;
    }

    /**
     *
     * @return
     */
    public long getOffset() {
        return offset;
    }

    /**
     *
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     *
     * @return
     */
    public String getArcfile() {
        return arcfile;
    }

    /**
     *
     * @param arcfile
     */
    public void setArcfile(String arcfile) {
        this.arcfile = arcfile;
    }

    /**
     *
     * @return
     */
    public String getDateString() {
        return dateString;
    }

    /**
     *
     * @param dateString
     */
    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.url);
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
        final CdxData other = (CdxData) obj;
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "IdxData{" + "name=" + name + ", url=" + url + ", mineType=" + mineType + ", httpResponse=" + httpResponse + ", offset=" + offset + ", arcfile=" + arcfile + ", dateString=" + dateString + '}';
    }
    
    
    
}
