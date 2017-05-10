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
import org.geowebcache.layer.TileLayer;
import org.geowebcache.rest.RestletException;
import org.geowebcache.rest.seed.GWCSeedingRestlet;
import org.geowebcache.rest.seed.SeedFormRestlet;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.TileBreeder;
import org.json.JSONArray;
import org.json.JSONException;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;

public class ValidateRestlet extends GWCSeedingRestlet {
    @SuppressWarnings("unused")
    private static Log log = LogFactory.getLog(SeedFormRestlet.class);

    private TileBreeder seeder;

    private QuotaStoreProvider quotaStoreProvider;

    private QuotaStore store;

    public void doGet(Request req, Response resp) throws RestletException {
        handleRequest(req, resp, null);
    }

    /**
     * Handle a POST request
     */
    public void doPost(Request req, Response resp) throws RestletException, IOException {
        handleRequest(req, resp, null);
    }

    protected void handleRequest(Request req, Response resp, Object obj) {
        String layerName = null;
        try {
            layerName = URLDecoder.decode((String) req.getAttributes().get("layer"), "UTF-8");
        } catch (UnsupportedEncodingException uee) {
        }

        try {
            GWCTask[] tasks = createTasks(seeder.findTileLayer(layerName));
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

    private GWCTask[] createTasks(TileLayer tl) {
        ValidateTask task = new ValidateTask(seeder.getStorageBroker(), store, tl);
        
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
