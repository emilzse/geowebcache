package org.geowebcache.diskquota;

import java.util.Arrays;

import org.geowebcache.diskquota.storage.TileSet;

public class UsageStats {

    private final TileSet tileSet;

    private final long[] tileIndex;

    private final double[] bbox;

    private final int epsgId;

    public UsageStats(TileSet tileset, long[] tileIndex, double[] bbox, int epsgId) {
        this.tileSet = tileset;
        this.tileIndex = tileIndex;
        this.bbox = bbox;
        this.epsgId = epsgId;
    }

    public TileSet getTileSet() {
        return tileSet;
    }

    public long[] getTileIndex() {
        return tileIndex;
    }

    public double[] getBbox() {
        return bbox;
    }

    public int getEpsgId() {
        return epsgId;
    }

    @Override
    public String toString() {
        return new StringBuilder("[").append(tileSet.toString()).append(", ")
                .append(Arrays.toString(tileIndex)).append("]").toString();
    }
}