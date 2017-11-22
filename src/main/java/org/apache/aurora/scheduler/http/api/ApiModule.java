/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.http.api;

import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.google.inject.servlet.ServletModule;
import net.morimekta.providence.serializer.BaseSerializerProvider;
import net.morimekta.providence.server.ProvidenceServlet;
import net.morimekta.providence.thrift.TBinaryProtocolSerializer;
import net.morimekta.providence.thrift.TJsonProtocolSerializer;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.scheduler.http.CorsFilter;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.util.resource.Resource;

public class ApiModule extends ServletModule {
  public static final String API_PATH = "/api";

  @Parameters(separators = "=")
  public static class Options {
    /**
     * Set the {@code Access-Control-Allow-Origin} header for API requests. See
     * http://www.w3.org/TR/cors/
     */
    @Parameter(names = "-enable_cors_for",
        description = "List of domains for which CORS support should be enabled.")
    public String enableCorsFor;
  }

  private final Options options;

  public ApiModule(Options options) {
    this.options = options;
  }

  private static final String API_CLIENT_ROOT = Resource
      .newClassPathResource("org/apache/aurora/scheduler/gen/client")
      .toString();

  @Override
  protected void configureServlets() {
    if (options.enableCorsFor != null) {
      filter(API_PATH).through(new CorsFilter(options.enableCorsFor));
    }
    serve(API_PATH).with(ProvidenceServlet.class);

    serve("/apiclient", "/apiclient/*")
        .with(new DefaultServlet(), ImmutableMap.<String, String>builder()
            .put("resourceBase", API_CLIENT_ROOT)
            .put("pathInfoOnly", "true")
            .put("dirAllowed", "false")
            .build());
  }

  @Provides
  @Singleton
  ProvidenceServlet provideApiThriftServlet(AnnotatedAuroraAdmin schedulerThriftInterface) {
    /*
     * For backwards compatibility the servlet is configured to assume `application/x-thrift` and
     * `application/json` have TJSON bodies.
     *
     * Requests that have the registered MIME type for apache thrift are mapped to their respective
     * protocols. See
     * http://www.iana.org/assignments/media-types/application/vnd.apache.thrift.binary and
     * http://www.iana.org/assignments/media-types/application/vnd.apache.thrift.json for details.
     *
     * Responses have the registered MIME type so the client can decode appropriately.
     *
     * The Accept header is used to determine the response type. By default JSON is sent for any
     * value except for the binary thrift header.
     */

    return new ProvidenceServlet(
        new AuroraAdmin.Processor(schedulerThriftInterface),
        new SerializerProvider());
  }

  private static class SerializerProvider extends BaseSerializerProvider {
    SerializerProvider() {
      super(TBinaryProtocolSerializer.ALT_MEDIA_TYPE);

      register(new TBinaryProtocolSerializer(true), TBinaryProtocolSerializer.MEDIA_TYPE);
      register(new TJsonProtocolSerializer(),
          TJsonProtocolSerializer.MEDIA_TYPE,
          TBinaryProtocolSerializer.ALT_MEDIA_TYPE,
          MediaType.APPLICATION_JSON);
    }
  }
}
