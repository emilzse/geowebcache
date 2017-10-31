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
package org.geowebcache.rest.controller.seed.invalidate;

import javax.servlet.http.HttpServletRequest;

import org.geowebcache.rest.exception.RestException;
import org.geowebcache.seed.InvalidateRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Component
@RestController
@RequestMapping(path="${gwc.context.suffix:}/rest")
public class InvalidateController {
    
    @Autowired
    private InvalidateService invalidateService;
    
    @ExceptionHandler(RestException.class)
    public ResponseEntity<?> handleRestException(RestException ex) {
        return new ResponseEntity<Object>(ex.toString(), ex.getStatus());
    }
    

    /**
     * GET method for creating invalidate job for the provided layer
     * @param req
     * @param layer
     * @param zxy z/x/y
     * @param jobId unique id to be used to keep track of deleted tiles for this job. Use same id for validate
     * @return
     */
    @RequestMapping(value = "/invalidate/{layer}.json", method = RequestMethod.GET)
    public ResponseEntity<?> doGet(HttpServletRequest req, @PathVariable String layer, @RequestParam("zxy") String zxy, @RequestParam("jobId") String jobId, @RequestParam(value = "poolSize", defaultValue = "1") int poolSize) {
        if (zxy.isEmpty() || zxy.trim().isEmpty()) {
            return new ResponseEntity<Object>("zxy must be provided", HttpStatus.BAD_REQUEST);
        }

        return invalidateService.handleRequest(layer, new InvalidateRequest(zxy), jobId, poolSize);
    }
    
    /**
     * Handle a POST request. 
     * 
     * @param req
     * @param layer
     * @param jobId unique id to be used to keep track of deleted tiles for this job. Use same id for validate
     * @param body [xml={@link InvalidateRequest},json={@link InvalidateConfigRequest}]
     * @return
     */
    @RequestMapping(value = "/invalidate/{layer}/{jobId:.+}", method = RequestMethod.POST)
    public ResponseEntity<?> doPost(HttpServletRequest req, @PathVariable String layer, @PathVariable String jobId, @RequestBody String body, @RequestParam(value = "poolSize", defaultValue = "1") int poolSize) {
        String formatExtension = jobId.indexOf(".") == -1 ? "xml" : jobId.substring(jobId.indexOf(".") + 1);
        jobId = jobId.indexOf(".") == -1 ? jobId : jobId.substring(0, jobId.indexOf("."));
        
        return invalidateService.handleRequest(layer, formatExtension, body, jobId, poolSize);
    }

}
