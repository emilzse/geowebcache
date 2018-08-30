package org.geowebcache.seed;

import java.util.Arrays;
import org.geowebcache.grid.BoundingBox;

public class InvalidateRequest {

    private InvalidateConfig[] config;

    public InvalidateRequest(String zxy) {
        int[] arr = Arrays.stream(zxy.split("/")).mapToInt(Integer::parseInt).toArray();

        BoundingBox box = tile2boundingBox(arr);

        this.config = new InvalidateConfig[] {new InvalidateConfig(box, 4326, arr[2])};
    }

    public InvalidateRequest(InvalidateConfig[] items) {
        this.config = items;
    }

    InvalidateRequest() {}

    /** @return objects with information what to invalidate */
    public InvalidateConfig[] getInvalidateItems() {
        return config;
    }

    // OSM http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Java

    private BoundingBox tile2boundingBox(final int[] zxy) {
        int zoom = zxy[0];
        int x = zxy[1];
        int y = zxy[2];

        double maxY = tile2lat(y, zoom);
        double minY = tile2lat(y + 1, zoom);
        double minX = tile2lon(x, zoom);
        double maxX = tile2lon(x + 1, zoom);

        return new BoundingBox(minX, minY, maxX, maxY);
    }

    private static double tile2lon(int x, int z) {
        return x / Math.pow(2.0, z) * 360.0 - 180;
    }

    private static double tile2lat(int y, int z) {
        double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }
}
