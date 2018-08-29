package org.geowebcache.diskquota;

import java.util.Arrays;
import org.geowebcache.diskquota.storage.TileSet;

public class UsageStats {

    private final TileSet tileSet;

    private final long[] tileIndex;

    private final String parametersKvp;

    public UsageStats(TileSet tileset, long[] tileIndex, String parametersKvp) {
        this.tileSet = tileset;
        this.tileIndex = tileIndex;
        this.parametersKvp = parametersKvp;
    }

    public TileSet getTileSet() {
        return tileSet;
    }

    public long[] getTileIndex() {
        return tileIndex;
    }

    public String getParametersKvp() {
        return parametersKvp;
    }

    @Override
    public String toString() {
        return new StringBuilder("[")
                .append(tileSet.toString())
                .append(", ")
                .append(Arrays.toString(tileIndex))
                .append("]")
                .toString();
    }
}
