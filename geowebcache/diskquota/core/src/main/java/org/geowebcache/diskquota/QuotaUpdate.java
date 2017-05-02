package org.geowebcache.diskquota;

import org.geowebcache.diskquota.storage.TileSet;

public class QuotaUpdate {

    private final TileSet tileSet;

    private long size;

    private long[] tileIndex;

    private int epsgId;

    private double[] bbox;

    /**
     * 
     * @param layerName
     * @param gridsetId
     * @param blobFormat
     * @param parametersId
     * @param size
     *            bytes to add or subtract from a quota: positive value increase quota, negative
     *            value decreases it
     * @param tileIdex
     */
    public QuotaUpdate(String layerName, String gridsetId, String blobFormat, String parametersId,
            long size, long[] tileIndex, int epsgId, double[] bbox) {
        this(new TileSet(layerName, gridsetId, blobFormat, parametersId), size, tileIndex, epsgId,
                bbox);
    }

    public QuotaUpdate(TileSet tileset, long quotaUpdateSize, long[] tileIndex, int epsgId,
            double[] bbox) {
        this.tileSet = tileset;
        this.size = quotaUpdateSize;
        this.tileIndex = tileIndex;
        this.epsgId = epsgId;
        this.bbox = bbox;
    }

    public TileSet getTileSet() {
        return tileSet;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long[] getTileIndex() {
        return tileIndex;
    }

    public int getEpsgId() {
        return epsgId;
    }

    public double[] getBbox() {
        return bbox;
    }

    @Override
    public String toString() {
        return new StringBuilder("[").append(tileSet.toString()).append(", ").append(size)
                .append(" bytes]").toString();
    }
}