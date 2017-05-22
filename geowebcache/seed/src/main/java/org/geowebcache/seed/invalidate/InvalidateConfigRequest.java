package org.geowebcache.seed.invalidate;

import java.util.Arrays;

public class InvalidateConfigRequest {

    public double[] bbox;
    public int epsgId;
    public int scaleLevel;
    
    InvalidateConfigRequest() {
    }
    
    public InvalidateConfigRequest(double[] bbox, int epsgId, int scaleLevel) {
        super();
        this.bbox = bbox;
        this.epsgId = epsgId;
        this.scaleLevel = scaleLevel;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("CRS=EPSG:").append(Integer.toString(epsgId)).append(", scaleLevel=").append(Integer.toString(scaleLevel)).append(", BBOX=").append(Arrays.toString(bbox)).toString();
    }
    
}
