package org.geowebcache.seed;

import org.geowebcache.grid.BoundingBox;

public class InvalidateConfig {

    public BoundingBox bounds;
    public int epsgId;
    public int scaleLevel;
    
    InvalidateConfig() {
    }
    
    public InvalidateConfig(BoundingBox bbox, int epsgId, int scaleLevel) {
        super();
        this.bounds = bbox;
        this.epsgId = epsgId;
        this.scaleLevel = scaleLevel;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("CRS=EPSG:").append(Integer.toString(epsgId)).append(", scaleLevel=").append(Integer.toString(scaleLevel)).append(", BBOX=").append(bounds == null ? "" : bounds.toString()).toString();
    }
     
}
