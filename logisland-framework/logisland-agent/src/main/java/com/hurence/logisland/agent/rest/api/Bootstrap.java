package com.hurence.logisland.agent.rest.api;

import io.swagger.jaxrs.config.SwaggerContextService;
import io.swagger.models.Contact;
import io.swagger.models.Info;
import io.swagger.models.License;
import io.swagger.models.Swagger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

public class Bootstrap extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    @Override
    public void init(ServletConfig config) throws ServletException {
        Info info = new Info()
                .title("Swagger Server")
                .description("REST API for logisland agent")
                .termsOfService("")
                .contact(new Contact()
                        .email("bailet.thomas@gmail.com"))
                .license(new License()
                        .name("")
                        .url(""));

        logger.info("starting logisland Agent");


        ServletContext context = config.getServletContext();
        Swagger swagger = new Swagger().info(info);

        new SwaggerContextService().withServletConfig(config).updateSwagger(swagger);
    }
}
