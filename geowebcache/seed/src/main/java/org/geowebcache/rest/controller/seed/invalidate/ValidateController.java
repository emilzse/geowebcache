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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Component
@RestController
@RequestMapping(path="${gwc.context.suffix:}/rest")
public class ValidateController {
    
    @Autowired
    ValidateService validateService;
    
    @ExceptionHandler(RestException.class)
    public ResponseEntity<?> handleRestException(RestException ex) {
        return new ResponseEntity<Object>(ex.toString(), ex.getStatus());
    }
    

    /**
     * GET method for creating invalidate job for the provided layer
     * @param req
     * @param layer
     * @param maxZoomLevel max zoom level to validate (important if big extent on low zoom levels where tile pages can include huge amount of tiles)
     * @return
     */
    @RequestMapping(value = "/validate/{layer}/{jobId}", method = RequestMethod.GET)
    public ResponseEntity<?> doGet(HttpServletRequest req, @PathVariable String layer, @PathVariable String jobId, @RequestParam(value = "poolSize", defaultValue = "1") int poolSize) {
        
        return validateService.handleRequest(layer, jobId, poolSize);
    }
    
}
