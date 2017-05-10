package org.geowebcache.seed.invalidate;

public class InvalidateRequest {

    private String[] zxy;

    private int epsgId = 4326;

    public InvalidateRequest() {
    }

    public InvalidateRequest(String[] zxy) {
        this.zxy = zxy;
    }

    /**
     * @return list of z/x/y
     */
    public String[] getZxy() {
        return zxy;
    }

}
