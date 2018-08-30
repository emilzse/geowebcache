/**
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * <p>You should have received a copy of the GNU Lesser General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Emil Zail
 */
package org.geowebcache.rest.controller.seed.invalidate;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.TileBreeder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class ValidateService extends AbsService {

    //    private static Log log = LogFactory.getLog(ValidateService.class);

    @Autowired TileBreeder seeder;

    protected ResponseEntity<?> handleRequest(String layerName, String jobId, int poolSize) {
        try {
            GWCTask[] tasks = createTasks(seeder.findTileLayer(layerName), jobId, poolSize);
            seeder.dispatchTasks(tasks);

            return new ResponseEntity<Object>(
                    Arrays.stream(tasks).mapToLong(GWCTask::getTaskId).toArray(), HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<Object>(e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (GeoWebCacheException e) {
            return new ResponseEntity<Object>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private GWCTask[] createTasks(TileLayer tl, String jobId, int poolSize)
            throws GeoWebCacheException {
        ValidateTask task =
                new ValidateTask(
                        seeder.getStorageBroker(),
                        tl,
                        dataSource,
                        tableName,
                        jobId,
                        seeder.getMaxPoolSize() < poolSize ? seeder.getMaxPoolSize() : poolSize);

        AtomicLong failureCounter = new AtomicLong();
        AtomicInteger sharedThreadCount = new AtomicInteger();

        task.setFailurePolicy(
                seeder.getTileFailureRetryCount(),
                seeder.getTileFailureRetryWaitTime(),
                seeder.getTotalFailuresBeforeAborting(),
                failureCounter);

        task.setThreadInfo(sharedThreadCount, 0);

        return new GWCTask[] {task};
    }

    public void setTileBreeder(TileBreeder seeder) {
        this.seeder = seeder;
    }
}
