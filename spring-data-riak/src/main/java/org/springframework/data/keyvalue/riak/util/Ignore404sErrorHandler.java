/*
 * Copyright (c) 2011 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2011 by NPC International, Inc. or the
 * original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.util;

import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.DefaultResponseErrorHandler;

import java.io.IOException;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class Ignore404sErrorHandler extends DefaultResponseErrorHandler {

  @Override
  protected boolean hasError(HttpStatus statusCode) {
    if (statusCode != HttpStatus.NOT_FOUND) {
      return super.hasError(statusCode);
    } else {
      return false;
    }
  }

  @Override
  public void handleError(ClientHttpResponse response) throws IOException {
    // Ignore 404s entirely
    if (response.getStatusCode() != HttpStatus.NOT_FOUND) {
      super.handleError(response);
    }
  }

}
