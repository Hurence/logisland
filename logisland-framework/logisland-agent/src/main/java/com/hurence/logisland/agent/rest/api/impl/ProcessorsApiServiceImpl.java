package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.NotFoundException;
import com.hurence.logisland.agent.rest.api.ProcessorsApiService;
import com.hurence.logisland.kafka.registry.KafkaRegistry;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.InputStream;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:53:12.750+01:00")
public class ProcessorsApiServiceImpl extends ProcessorsApiService {


    private static Logger logger = LoggerFactory.getLogger(ProcessorsApiServiceImpl.class);

    public ProcessorsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

    @Override
    public Response getProcessors(SecurityContext securityContext) throws NotFoundException {
        String jsonPlugins = loadFileContentAsString(JSON_PLUGINS_FILE, "UTF-8");
        // do some magic!
        return Response.ok().entity(jsonPlugins).build();
    }


    private static String JSON_PLUGINS_FILE = "processors.json";

    /**
     * load logs from resources as a string or return null if an error occurs
     */
    public static String loadFileContentAsString(String path, String encoding) {
        try {
            final InputStream is = ProcessorsApiServiceImpl.class.getClassLoader().getResourceAsStream(path);
            assert is != null;
            byte[] encoded = IOUtils.toByteArray(is);
            is.close();
            return new String(encoded, encoding);
        } catch (Exception e) {
            logger.error(String.format("Could not load json file %s and convert to string", path), e);
            return null;
        }
    }

    /**
     * load file from resources as a bytes array or return null if an error occurs
     */
    public static byte[] loadFileContentAsBytes(String path) {
        try {
            final InputStream is = ProcessorsApiServiceImpl.class.getClassLoader().getResourceAsStream(path);
            assert is != null;
            byte[] encoded = IOUtils.toByteArray(is);
            is.close();
            return encoded;
        } catch (Exception e) {
            logger.error(String.format("Could not load file %s and convert it to a bytes array", path), e);
            return null;
        }
    }
}
