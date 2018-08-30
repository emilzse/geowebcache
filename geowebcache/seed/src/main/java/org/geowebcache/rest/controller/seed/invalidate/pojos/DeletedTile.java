package org.geowebcache.rest.controller.seed.invalidate.pojos;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.storage.StorageBroker;

public class DeletedTile {

    public final String layerName;
    public final String gridSetId;
    public final long x;
    public final long y;
    public final long z;
    public final String mimeType;
    public final Map<String, String> parameters;

    public DeletedTile(
            String layerName,
            String gridSetId,
            long x,
            long y,
            long z,
            String mimeType,
            Map<String, String> parameters) {
        super();
        this.layerName = layerName;
        this.gridSetId = gridSetId;
        this.x = x;
        this.y = y;
        this.z = z;
        this.mimeType = mimeType;
        this.parameters = parameters == null ? Collections.emptyMap() : parameters;
    }

    public ConveyorTile createTile(StorageBroker storageBroker) throws MimeException {
        return new ConveyorTile(
                storageBroker,
                layerName,
                gridSetId,
                new long[] {x, y, z},
                MimeType.createFromFormat(mimeType),
                parameters);
    }

    @Override
    public String toString() {
        return new StringBuilder("layer=")
                .append(layerName)
                .append(",gridSetId=")
                .append(gridSetId)
                .append(",xyz=")
                .append(Arrays.toString(new long[] {x, y, z}))
                .append(",format=")
                .append(mimeType)
                .toString();
    }
}
