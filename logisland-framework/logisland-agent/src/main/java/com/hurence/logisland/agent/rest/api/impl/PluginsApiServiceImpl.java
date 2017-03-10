package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.ApiResponseMessage;
import com.hurence.logisland.agent.rest.api.NotFoundException;
import com.hurence.logisland.agent.rest.api.PluginsApiService;
import com.hurence.logisland.kakfa.registry.KafkaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-03T16:47:02.913+01:00")
public class PluginsApiServiceImpl extends PluginsApiService {

    private static Logger logger = LoggerFactory.getLogger(PluginsApiServiceImpl.class);
    public PluginsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

    private static String JSON_PLUGINS_FILE = "plugins.json";
    /**
     * load logs from ressource as a string or return null if an error occurs
     */
    public static String loadFileContentAsString(String path, String encoding) {
        try {
            final URL url = PluginsApiServiceImpl.class.getClassLoader().getResource(path);
            assert url != null;
            logger.debug("try to load plugins definition from file {} ", url);
            byte[] encoded = Files.readAllBytes(Paths.get(new File(url.toURI()).getAbsolutePath()));
            return new String(encoded, encoding);
        } catch (Exception e) {
            logger.error(String.format("Could not load json file %s and convert to string", path), e);
            return null;
        }
    }

    @Override
    public Response getPlugins(SecurityContext securityContext) throws NotFoundException {


        String jsonPlugins = loadFileContentAsString(JSON_PLUGINS_FILE, "UTF-8");
        // do some magic!
        return Response.ok().entity(jsonPlugins).build();
    }
}
