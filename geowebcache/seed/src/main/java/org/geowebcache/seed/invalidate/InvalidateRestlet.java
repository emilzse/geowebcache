/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author Emil Zail
 */
package org.geowebcache.seed.invalidate;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.diskquota.QuotaStore;
import org.geowebcache.diskquota.QuotaStoreProvider;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.io.GeoWebCacheXStream;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.rest.RestletException;
import org.geowebcache.rest.seed.GWCSeedingRestlet;
import org.geowebcache.rest.seed.SeedFormRestlet;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.InvalidateConfig;
import org.geowebcache.seed.InvalidateRequest;
import org.geowebcache.seed.TileBreeder;
import org.json.JSONArray;
import org.json.JSONException;
import org.restlet.data.Form;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

public class InvalidateRestlet extends GWCSeedingRestlet {
    
    private static Log log = LogFactory.getLog(SeedFormRestlet.class);

    private TileBreeder seeder;

    private QuotaStoreProvider quotaStoreProvider;

    private QuotaStore store;

    public void doGet(Request req, Response resp) throws RestletException {
        Form form = req.getResourceRef().getQueryAsForm();
        String zxy = form.getFirstValue("zxy", true);

        if (zxy == null || zxy.isEmpty() || zxy.trim().isEmpty()) {
            throw new RestletException("zxy must be provided", Status.CLIENT_ERROR_BAD_REQUEST);
        }

        handleRequest(req, resp, new InvalidateRequest(zxy));
    }
    
    /**
     * Handle a POST request. 
     * <p>xml={@link InvalidateRequest}
     * <br>json={@link InvalidateConfigRequest}
     * 
     */
    public void doPost(Request req, Response resp) throws RestletException, IOException {
        String formatExtension = (String) req.getAttributes().get("extension");

        XStream xs = configXStream(new GeoWebCacheXStream(new DomDriver()));

        Object obj = null;
        
        if (formatExtension==null || formatExtension.equalsIgnoreCase("xml")) {
            obj = xs.fromXML(req.getEntity().getStream());
        } else if (formatExtension.equalsIgnoreCase("json")) {
            String text = req.getEntity().getText();
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                InvalidateConfigRequest[] ic = objectMapper.readValue(text, InvalidateConfigRequest[].class);
                InvalidateConfig[] configs = Arrays.stream(ic).map(o -> new InvalidateConfig(new BoundingBox(o.bbox[0], o.bbox[1], o.bbox[2], o.bbox[3]), o.epsgId, o.scaleLevel)).toArray(InvalidateConfig[]::new);
                
                obj = new InvalidateRequest(configs);
            } catch (Exception e) {
                log.info(e.getMessage(), e);
                
                throw new RestletException(e.getMessage(), Status.SERVER_ERROR_INTERNAL);
            }
        } else {
            throw new RestletException("Format extension unknown or not specified: "
                    + formatExtension, Status.CLIENT_ERROR_BAD_REQUEST);
        }

        handleRequest(req, resp, obj);

    }

    protected void handleRequest(Request req, Response resp, Object obj) {
        final InvalidateRequest ir = (InvalidateRequest) obj;
        String layerName = null;
        try {
            layerName = URLDecoder.decode((String) req.getAttributes().get("layer"), "UTF-8");
        } catch (UnsupportedEncodingException uee) {
        }

        try {
            GWCTask[] tasks = createTasks(seeder.findTileLayer(layerName), ir);
            seeder.dispatchTasks(tasks);

            resp.setEntity(new JsonRepresentation(
                    new JSONArray(Arrays.stream(tasks).mapToLong(GWCTask::getTaskId).toArray())));
        } catch (IllegalArgumentException e) {
            throw new RestletException(e.getMessage(), Status.CLIENT_ERROR_BAD_REQUEST);
        } catch (GeoWebCacheException e) {
            throw new RestletException(e.getMessage(), Status.SERVER_ERROR_INTERNAL);
        } catch (JSONException e) {
            throw new RestletException(e.getMessage(), Status.SERVER_ERROR_INTERNAL);
        }

    }

    private GWCTask[] createTasks(TileLayer tl, InvalidateRequest ir) {
        InvalidateTask task = new InvalidateTask(seeder.getStorageBroker(), store, tl, ir.getInvalidateItems());
        
        AtomicLong failureCounter = new AtomicLong();
        AtomicInteger sharedThreadCount = new AtomicInteger();
        
        task.setFailurePolicy(seeder.getTileFailureRetryCount(),
                seeder.getTileFailureRetryWaitTime(), seeder.getTotalFailuresBeforeAborting(),
                failureCounter);

        task.setThreadInfo(sharedThreadCount, 0);

        return new GWCTask[] { task };
    }

    public void setTileBreeder(TileBreeder seeder) {
        this.seeder = seeder;
    }

    public void setQuotaStoreProvider(QuotaStoreProvider quotaStoreProvider) {
        this.quotaStoreProvider = quotaStoreProvider;

        try {
            store = quotaStoreProvider.getQuotaStore();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
