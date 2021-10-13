/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.ImageHtmlEmail;
import org.apache.commons.mail.SimpleEmail;
import org.apache.commons.mail.resolver.DataSourceClassPathResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SendMail Processor.
 * This processor is able to send mails from the incoming records.
 * A minimum processor configuration is required (mainly smtp server info and some mail sender info)
 * Only records with either FIELD_MAIL_TEXT or FIELD_MAIL_HTML fields set will generate a mail.
 * - If FIELD_MAIL_TEXT is set, it holds the text content of the mail to be sent
 * - If FIELD_MAIL_HTML is set, it informs the processor to use the HTML template of its configuration for sending the
 *   mail. Any parameter defined in the template will be retrieved from the fields with the same name embedded in the
 *   record. Every parameters defined in the template with the form ${xxx} must be present in the record (the record
 *   must hold a xxx field).
 * 
 * If it is enabled in processor configuration (allow_overwrite), it is possible to overwrite some config properties
 * directly in the record with some specific fields (FIELD_MAIL_TO, FIELD_MAIL_SUBJECT...)
 * 
 * Note: you can use an html template using embedded image. Images should then be in the class path (in a jar file)
 * and the format of src attribute in the img html tag should be like:
 * <img src="path/in/classpath/spectacular_image.gif" alt="Spectacular Image">
 */
@Tags({"smtp", "email", "e-mail", "mail", "mailer", "sendmail", "message", "alert", "html"})
@CapabilityDescription(
        "The SendMail processor is aimed at sending an email (like for instance an alert email) from an incoming record."
        + " There are three ways an incoming record can generate an email according to the special fields it must embed."
        + " Here is a list of the record fields that generate a mail and how they work:\n" +
                "\n"
        + "- **mail_text**: this is the simplest way for generating a mail. If present, this field means to use its content (value)"
        + " as the payload of the mail to send. The mail is sent in text format if there is only this special field in the"
        + " record. Otherwise, used with either mail_html or mail_use_template, the content of mail_text is the aletrnative"
        + " text to the HTML mail that is generated.\n" +
                "\n"
        + "- **mail_html**: this field specifies that the mail should be sent as HTML and the value of the field is mail"
        + " payload. If mail_text is also present, its value is used as the alternative text for the mail. mail_html"
        + " cannot be used with mail_use_template: only one of those two fields should be present in the record.\n" +
                "\n"
        + "- **mail_use_template**: If present, this field specifies that the mail should be sent as HTML and the HTML content"
        + " is to be generated from the template in the processor configuration key **html.template**. The template can contain"
        + " parameters which must also be present in the record as fields. See documentation of html.template for further"
        + " explanations. mail_use_template cannot be used with mail_html: only one of those two fields should be present"
        + " in the record.\n" +
                "\n"
        + " If **allow_overwrite** configuration key is true, any mail.* (dot format) configuration key may be overwritten with a matching"
        + " field in the record of the form mail_* (underscore format). For instance if allow_overwrite is true and mail.to is"
        + " set to config_address@domain.com, a record generating a mail with a mail_to field set to record_address@domain.com"
        + " will send a mail to record_address@domain.com.\n" +
                "\n"
        + " Apart from error records (when he is unable to process the incoming record or to send the mail), this processor"
        + " is not expected to produce any output records.")
@ExtraDetailFile("./details/common-processors/SendMail-Detail.rst")
public class SendMail extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(SendMail.class);
    
    // Easy trick to not allow debugging without changing the logger level but instead using a configuration key
    private boolean debug = false;

    private String smtpServer = null;
    private int smtpPort = 25;
    private String smtpSecurityUsername = null;
    private String smtpSecurityPassword = null;
    private boolean smtpSecuritySsl = false;

    private static final String DEFAULT_FROM_NAME = "Logisland";
    private static final String DEFAULT_SUBJECT = "[LOGISLAND] Automatic email";

    private String[] mailTos = new String[]{};
    private String mailFromAddress = null;
    private String mailFromName = DEFAULT_FROM_NAME;
    private String mailBounceAddress = null;
    private String mailReplyToAddress = null;
    private String mailSubject = DEFAULT_SUBJECT;
    private boolean allowFieldsOverwriting = true;
    
    // HTML template as stated in the configuration
    private String htmlTemplate = null;
    
    // HTML form derived from htmlTemplate, ready for parameters injection
    private MessageFormat htmlForm = null;
    
    // List of parameters for the HTML template. They must be present in the record as fields.
    private List<String> parameterNames = new ArrayList<String>();

    /**
     * Definitions for the fields of the incoming record
     */
    
    private static final String FIELD_PREFIX = "mail";
    
    /**
     *     Fields that trigger an email
     */
    // This holds the content of the mail to be sent as text.
    public static final String FIELD_MAIL_TEXT = FIELD_PREFIX + "_text";
    // This holds the content of the mail to be sent as html.
    public static final String FIELD_MAIL_HTML = FIELD_PREFIX + "_html";
    // If this field is present (no matter his content), the HTML template is used to send the mail.
    public static final String FIELD_MAIL_USE_TEMPLATE = FIELD_PREFIX + "_use_template";
    
    /**
     *     Optional fields that overwrite behavior from configuration
     */
    // May be used to overwrite mail.to configured in processor
    public static final String FIELD_MAIL_TO = FIELD_PREFIX + "_to";
    // May be used to overwrite mail.from.address configured in processor 
    public static final String FIELD_MAIL_FROM_ADDRESS = FIELD_PREFIX + "_from_address";    
    // May be used to overwrite mail.from.name configured in processor
    public static final String FIELD_MAIL_FROM_NAME = FIELD_PREFIX + "_from_name";
    // May be used to overwrite mail.bounce.address configured in processor 
    public static final String FIELD_MAIL_BOUNCE_ADDRESS = FIELD_PREFIX + "_bounce_address"; 
    // May be used to overwrite mail.replyto.address configured in processor 
    public static final String FIELD_MAIL_REPLYTO_ADDRESS = FIELD_PREFIX + "_replyto_address";
    // May be used to overwrite mail.subject configured in processor 
    public static final String FIELD_MAIL_SUBJECT = FIELD_PREFIX + "_subject";

    /**
     * Configuration keys
     */

    // Easy trick to not allow debugging without changing the logger level but instead using a configuration key
    private static final String KEY_DEBUG = "debug";

    private static final String KEY_SMTP_SERVER = "smtp.server";
    private static final String KEY_SMTP_PORT = "smtp.port";
    private static final String KEY_SMTP_SECURITY_USERNAME = "smtp.security.username";
    private static final String KEY_SMTP_SECURITY_PASSWORD = "smtp.security.password";
    private static final String KEY_SMTP_SECURITY_SSL = "smtp.security.ssl";

    private static final String KEY_MAIL_TO = "mail.to";
    private static final String KEY_MAIL_FROM_ADDRESS = "mail.from.address";
    private static final String KEY_MAIL_FROM_NAME = "mail.from.name";
    private static final String KEY_MAIL_BOUNCE_ADDRESS = "mail.bounce.address";
    private static final String KEY_MAIL_REPLYTO_ADDRESS = "mail.replyto.address";
    private static final String KEY_MAIL_SUBJECT = "mail.subject";
    
    private static final String KEY_ALLOW_OVERWRITE = "allow_overwrite";
    
    private static final String KEY_HTML_TEMPLATE = "html.template";
    
    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug. If enabled, debug information are written to stdout.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor SMTP_SERVER = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_SERVER)
            .description("FQDN, hostname or IP address of the SMTP server to use.")
            .required(true)
            .build();
    
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_PORT)
            .description("TCP port number of the SMTP server to use.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("25")
            .required(false)
            .build();
    
    public static final PropertyDescriptor SMTP_SECURITY_USERNAME = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_SECURITY_USERNAME)
            .description("SMTP username.")
            .required(false)
            .build();
    
    public static final PropertyDescriptor SMTP_SECURITY_PASSWORD = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_SECURITY_PASSWORD)
            .description("SMTP password.")
            .required(false)
            .build();
    
    public static final PropertyDescriptor SMTP_SECURITY_SSL = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_SECURITY_SSL)
            .description("Use SSL under SMTP or not (SMTPS). Default is false.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    
    public static final PropertyDescriptor MAIL_FROM_ADDRESS = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_FROM_ADDRESS)
            .description("Valid mail sender email address.")
            .required(true)
            .build();
    
    public static final PropertyDescriptor MAIL_FROM_NAME = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_FROM_NAME)
            .description("Mail sender name.")
            .required(false)
            .build();
    
    public static final PropertyDescriptor MAIL_BOUNCE_ADDRESS = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_BOUNCE_ADDRESS)
            .description("Valid bounce email address (where error mail is sent if the mail is refused by the recipient"
                    + " server).")
            .required(true)
            .build();
    
    public static final PropertyDescriptor MAIL_REPLYTO_ADDRESS = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_REPLYTO_ADDRESS)
            .description("Reply to email address.")
            .required(false)
            .build();
    
    public static final PropertyDescriptor MAIL_SUBJECT = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_SUBJECT)
            .description("Mail subject.")
            .required(false)
            .defaultValue(DEFAULT_SUBJECT)
            .build();
    
    public static final PropertyDescriptor MAIL_TO = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_TO)
            .description("Comma separated list of email recipients. If not set, the record must have a "
            + FIELD_MAIL_TO + " field and " + KEY_ALLOW_OVERWRITE + " configuration key should be true.")
            .required(false)
            .build();
    
    public static final PropertyDescriptor ALLOW_OVERWRITE = new PropertyDescriptor.Builder()
            .name(KEY_ALLOW_OVERWRITE)
            .description("If true, allows to overwrite processor configuration with special record fields (" +
                    FIELD_MAIL_TO + ", " + FIELD_MAIL_FROM_ADDRESS  + ", " + FIELD_MAIL_FROM_NAME
                    + ", " + FIELD_MAIL_BOUNCE_ADDRESS + ", " + FIELD_MAIL_REPLYTO_ADDRESS + ", " + FIELD_MAIL_SUBJECT
                    +"). If false, special record fields are ignored and only processor configuration keys are used.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();
    
    public static final PropertyDescriptor HTML_TEMPLATE = new PropertyDescriptor.Builder()
            .name(KEY_HTML_TEMPLATE)
            .description("HTML template to use. It is used when the incoming record contains a " + FIELD_MAIL_USE_TEMPLATE
                    + " field. The template may contain some parameters. The parameter format in the template is of the"
                    + " form ${xxx}. For instance ${param_user} in the template means that a field named"
                    + " param_user must be present in the record and its value will replace the ${param_user} string"
                    + " in the HTML template when the mail will be sent. If some parameters are declared in the template"
                    + ", everyone of them must be present in the record as fields, otherwise the record will generate"
                    + " an error record. If an incoming record contains a " + FIELD_MAIL_USE_TEMPLATE + " field, a template"
                    + " must be present in the configuration and the HTML mail format will be used. If the record"
                    + " also contains a " + FIELD_MAIL_TEXT + " field, its content will be used as an alternative text"
                    + " message to be used in the mail reader program of the recipient if it does not supports HTML.")
            .required(false)
            .build();

    @Override
    public void init(final ProcessContext context) throws InitializationException
    {
        super.init(context);
        logger.debug("Initializing SendMail Processor");
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG);
        descriptors.add(SMTP_SERVER);
        descriptors.add(SMTP_PORT);
        descriptors.add(SMTP_SECURITY_USERNAME);
        descriptors.add(SMTP_SECURITY_PASSWORD);
        descriptors.add(SMTP_SECURITY_SSL);
        descriptors.add(MAIL_FROM_ADDRESS);
        descriptors.add(MAIL_FROM_NAME);
        descriptors.add(MAIL_BOUNCE_ADDRESS);
        descriptors.add(MAIL_REPLYTO_ADDRESS);
        descriptors.add(MAIL_SUBJECT);
        descriptors.add(MAIL_TO);
        descriptors.add(ALLOW_OVERWRITE);
        descriptors.add(HTML_TEMPLATE);

        return Collections.unmodifiableList(descriptors);
    }
  
    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        if (debug)
        {
            logger.info("SendMail Processor records input: " + records);
        }
        
        final Collection<Record> failedRecords = new ArrayList<>();

        /**
         * Transform the records into mails and send them
         */
        for (Record record : records)
        {            
            String mailText = getStringField(record, FIELD_MAIL_TEXT);
            boolean text = (mailText != null);
            String mailHtml = getStringField(record, FIELD_MAIL_HTML);
            boolean html = (mailHtml != null);
            Field mailUseTemplate = record.getField(FIELD_MAIL_USE_TEMPLATE);
            boolean useTemplate = (mailUseTemplate != null);

            if (html || text || useTemplate)
            {
                // Ok, there is a mail to send. First retrieve some potential embedded overwritten configuration fields 
               
                String[] finalMailTos = mailTos;
                String finalMailFromAddress = mailFromAddress;
                String finalMailFromName = mailFromName;
                String finalMailBounceAddress = mailBounceAddress;
                String finalMailReplyToAddress = mailReplyToAddress;
                String finalMailSubject = mailSubject;

                /**
                 * Overwrite some variables with special fields in the record if any and this is allowed
                 */
                if (allowFieldsOverwriting)
                {
                    String recordMailFromAddress = getStringField(record, FIELD_MAIL_FROM_ADDRESS);
                    if (recordMailFromAddress != null)
                    {
                        finalMailFromAddress = recordMailFromAddress;
                    }

                    String recordMailFromName = getStringField(record, FIELD_MAIL_FROM_NAME);
                    if (recordMailFromName != null)
                    {
                        finalMailFromName = recordMailFromName;
                    }
                    
                    String recordMailBounceAddress = getStringField(record, FIELD_MAIL_BOUNCE_ADDRESS);
                    if (recordMailBounceAddress != null)
                    {
                        finalMailBounceAddress = recordMailBounceAddress;
                    }
                    
                    String recordMailReplyToAddress = getStringField(record, FIELD_MAIL_REPLYTO_ADDRESS);
                    if (recordMailReplyToAddress != null)
                    {
                        finalMailReplyToAddress = recordMailReplyToAddress;
                    }
                    
                    String recordMailSubject = getStringField(record, FIELD_MAIL_SUBJECT);
                    if (recordMailSubject != null)
                    {
                        finalMailSubject = recordMailSubject;
                    }
                    
                    String recordMailTo = getStringField(record, FIELD_MAIL_TO);
                    if (recordMailTo != null)
                    {
                        finalMailTos = parseMailTo(recordMailTo);
                    }
                }
                
                if (finalMailFromAddress == null)
                {
                    record.addError(ProcessError.UNKNOWN_ERROR.getName(), "No From address defined");
                    failedRecords.add(record);
                    continue;
                }

                if (finalMailBounceAddress == null)
                {
                    record.addError(ProcessError.UNKNOWN_ERROR.getName(), "No Bounce address defined");
                    failedRecords.add(record);
                    continue;
                }
                
                if (html || useTemplate)
                {
                    if (html && useTemplate)
                    {
                        record.addError(ProcessError.UNKNOWN_ERROR.getName(), "Record has both " + FIELD_MAIL_USE_TEMPLATE 
                                + " and " + FIELD_MAIL_HTML + " fields. Only one of them is expected.");
                        failedRecords.add(record);
                        continue;
                    }

                    // HTML mail
                    try {
                        
                        /**
                         * Create and fill the mail
                         */
                        
                        ImageHtmlEmail htmlEmail = new ImageHtmlEmail();

                        // Set From info
                        if (finalMailFromName != null)
                        {
                            htmlEmail.setFrom(finalMailFromAddress, finalMailFromName);
                        }
                        else
                        {
                            htmlEmail.setFrom(finalMailFromAddress);
                        }

                        htmlEmail.setBounceAddress(finalMailBounceAddress);

                        if (finalMailReplyToAddress != null)
                        {
                            htmlEmail.addReplyTo(finalMailReplyToAddress);
                        }
                        
                        htmlEmail.setSubject(finalMailSubject);
                        
                        // Allow to retrieve embedded images of the html template from the classpath (jar files)
                        htmlEmail.setDataSourceResolver(new DataSourceClassPathResolver());

                        // Compute final HTML body from template and record fields
                        String htmlBody = null;                        
                        if (useTemplate)
                        {
                            htmlBody = createHtml(record);  
                            if (htmlBody == null)
                            {
                                // Error. Error message has already been set in createHtml: just add to failed record and
                                // continue with next record
                                failedRecords.add(record);
                                continue;
                            }
                        } else
                        {
                            // html
                            htmlBody = mailHtml;
                        }

                        htmlEmail.setHtmlMsg(htmlBody);
                        
                        // Add alternative text mail if any defined
                        if (text)
                        {
                            htmlEmail.setTextMsg(mailText);
                        }

                        // Set To info
                        if (finalMailTos.length == 0)
                        {
                            record.addError(ProcessError.UNKNOWN_ERROR.getName(), "No mail recipient.");
                            failedRecords.add(record);
                            continue;
                        }
                        for(String mailTo : finalMailTos)
                        {
                            htmlEmail.addTo(mailTo);
                        }
                        
                        /**
                         * Set sending parameters
                         */
                       
                        htmlEmail.setHostName(smtpServer);
                        htmlEmail.setSmtpPort(smtpPort);
                        if ( (smtpSecurityUsername != null) && (smtpSecurityPassword != null) )
                        {
                            htmlEmail.setAuthentication(smtpSecurityUsername, smtpSecurityPassword);
                        }
                        htmlEmail.setSSLOnConnect(smtpSecuritySsl);
                        
                        // Send the mail
                        htmlEmail.send();
                    } catch (EmailException ex) {
                        record.addError(ProcessError.UNKNOWN_ERROR.getName(), "Unable to send email: " + ex.getMessage());
                        failedRecords.add(record);
                    }
                } else
                {
                    // Only text mail
                    try {
                        
                        /**
                         * Create and fill the mail
                         */
                        
                        SimpleEmail textEmail = new SimpleEmail();
                        
                        // Set From info
                        if (finalMailFromName != null)
                        {
                            textEmail.setFrom(finalMailFromAddress, finalMailFromName);
                        }
                        else
                        {
                            textEmail.setFrom(finalMailFromAddress);
                        }
                        
                        textEmail.setBounceAddress(finalMailBounceAddress);
                        if (finalMailReplyToAddress != null)
                        {
                            textEmail.addReplyTo(finalMailReplyToAddress);
                        }
                        
                        textEmail.setSubject(finalMailSubject);
                        textEmail.setMsg(mailText);

                        // Set To info
                        if (finalMailTos.length == 0)
                        {
                            record.addError(ProcessError.UNKNOWN_ERROR.getName(), "No mail recipient.");
                            failedRecords.add(record);
                            continue;
                        }
                        for(String mailTo : finalMailTos)
                        {
                            textEmail.addTo(mailTo);
                        }
                        
                        /**
                         * Set sending parameters
                         */
                       
                        textEmail.setHostName(smtpServer);
                        textEmail.setSmtpPort(smtpPort);
                        if ( (smtpSecurityUsername != null) && (smtpSecurityPassword != null) )
                        {
                            textEmail.setAuthentication(smtpSecurityUsername, smtpSecurityPassword);
                        }
                        textEmail.setSSLOnConnect(smtpSecuritySsl);
                        
                        // Send the mail
                        textEmail.send();
                    } catch (EmailException ex) {
                        record.addError(ProcessError.UNKNOWN_ERROR.getName(), "Unable to send email: " + ex.getMessage());
                        failedRecords.add(record);
                    }
                }
            }
        }

        if (debug)
        {
            logger.info("SendMail Processor records output: " + records);
        }
        return failedRecords;
    }
    
    /**
     * Create the HTML message from the html template and the potential parameters in the record
     * @param record The record to inspect to retrieve the parameters of the template (from the record fields)
     * @return The filled HTML template or null if an error occurred
     */
    private String createHtml(Record record)
    {
        // Special case if no parameters are expected, just return the template
        int nParams = parameterNames.size();
        if (nParams == 0)
        {
            // Template expects no args, just return it as message body.
            // For instance may be used if body is always the same but some mail things change like subject or mailto...
            return htmlTemplate;
        }
        
        Object[] templateParams = new Object[nParams];
        int index = 0;
        for(String parameter : parameterNames)
        {
            // Find the parameter as a field in the record
            Field field = record.getField(parameter);
            if (field == null)
            {
                // Field not found, error
                record.addError(ProcessError.UNKNOWN_ERROR.getName(), "Could not find expected HTML template parameter <"
                        + parameter + "> as field in the record.");
                return null;
            }
            
            templateParams[index] = field.getRawValue();
            
            index++;
        }
        
        // Now we have the parameters, fill the form and return matching HTML body
        return htmlForm.format(templateParams);
    }
    
    /**
     * Retrieve the record field value
     * @param fieldName The name of the string field
     * @return The value of the field or null if the field is not present in the record
     */
    private String getStringField(Record record, String fieldName)
    {
        Field field = record.getField(fieldName);
        if (field != null)
        {
            return field.asString();
        }
        else
        {
            return null;
        }
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(Configuration context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        logger.debug("customValidate");

        /**
         * If the mail.to configuration key is not set, allow_overwrite should be true so that we know to who we must
         * send the mail (record must then hold a mail_to field)
         */
        if (!context.getPropertyValue(MAIL_TO).isSet())
        {
            
            if (!context.getPropertyValue(ALLOW_OVERWRITE).asBoolean()) {
                validationResults.add(
                        new ValidationResult.Builder()
                            .explanation("if " + MAIL_TO.getName() + " is not set, " + ALLOW_OVERWRITE.getName() + " must be true"
                                    + " so that the record holds a " + FIELD_MAIL_TO + " field that is used.")
                            .valid(false)
                            .build());
            }
        }
        
        /**
         * Both username and password must be set or none of them
         */
        if ( (context.getPropertyValue(SMTP_SECURITY_USERNAME).isSet() && !context.getPropertyValue(SMTP_SECURITY_PASSWORD).isSet())
                || (!context.getPropertyValue(SMTP_SECURITY_USERNAME).isSet() && context.getPropertyValue(SMTP_SECURITY_PASSWORD).isSet()))
        {
            validationResults.add(
                    new ValidationResult.Builder()
                        .explanation("Both " + SMTP_SECURITY_USERNAME.getName() + " and " + SMTP_SECURITY_PASSWORD.getName() + " should be set"
                                + " or none of them.")
                        .valid(false)
                        .build());
        }
      
        return validationResults;
    }    
    
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

        logger.debug("property {} value changed from {} to {}", descriptor.getName(), oldValue, newValue);
        
        /**
         * Handle the DEBUG property
         */
        if (descriptor.equals(DEBUG))
        {
          if (newValue != null)
          {
              if (newValue.equalsIgnoreCase("true"))
              {
                  debug = true;
              }
          } else
          {
              debug = false;
          }
        }
        
        /**
         * Handle the SMTP_SERVER property
         */
        if (descriptor.equals(SMTP_SERVER))
        {
            smtpServer = newValue;
        }
        
        /**
         * Handle the SMTP_PORT property
         */
        if (descriptor.equals(SMTP_PORT))
        {
            int port = -1;
            try 
            {
                port = new Integer(newValue);
            } catch (NumberFormatException e)
            {
                // TODO something to do?
            }

            smtpPort = port;
        }
        
        /**
         * Handle the SMTP_SECURITY_USERNAME property
         */
        if (descriptor.equals(SMTP_SECURITY_USERNAME))
        {
            smtpSecurityUsername = newValue;
        }
        
        /**
         * Handle the SMTP_SECURITY_PASSWORD property
         */
        if (descriptor.equals(SMTP_SECURITY_PASSWORD))
        {
            smtpSecurityPassword = newValue;
        }
        
        /**
         * Handle the SMTP_SECURITY_SSL property
         */
        if (descriptor.equals(SMTP_SECURITY_SSL))
        {
          if (newValue != null)
          {
              if (newValue.equalsIgnoreCase("true"))
              {
                  smtpSecuritySsl = true;
              }
          } else
          {
              smtpSecuritySsl = false;
          }
        }
        
        /**
         * Handle the MAIL_FROM_ADDRESS property
         */
        if (descriptor.equals(MAIL_FROM_ADDRESS))
        {
            mailFromAddress = newValue;
        }
        
        /**
         * Handle the MAIL_FROM_NAME property
         */
        if (descriptor.equals(MAIL_FROM_NAME))
        {
            mailFromName = newValue;
        }
        
        /**
         * Handle the MAIL_BOUNCE_ADDRESS property
         */
        if (descriptor.equals(MAIL_BOUNCE_ADDRESS))
        {
            mailBounceAddress = newValue;
        }
        
        /**
         * Handle the MAIL_REPLYTO_ADDRESS property
         */
        if (descriptor.equals(MAIL_REPLYTO_ADDRESS))
        {
            mailReplyToAddress = newValue;
        }
        
        /**
         * Handle the MAIL_SUBJECT property
         */
        if (descriptor.equals(MAIL_SUBJECT))
        {
            mailSubject = newValue;
        }
        
        /**
         * Handle the MAIL_TO property
         */
        if (descriptor.equals(MAIL_TO))
        {
            mailTos = parseMailTo(newValue);
        }
        
        /**
         * Handle the ALLOW_OVERWRITE property
         */
        if (descriptor.equals(ALLOW_OVERWRITE))
        {
          if (newValue != null)
          {
              if (newValue.equalsIgnoreCase("true"))
              {
                  allowFieldsOverwriting = true;
              }
          } else
          {
              allowFieldsOverwriting = false;
          }
        }
        
        /**
         * Handle the HTML_TEMPLATE property
         */
        if (descriptor.equals(HTML_TEMPLATE))
        {
            htmlTemplate = newValue;
            prepareTemplateAndParameters(htmlTemplate);
        }
        
        if (debug)
        {
            displayConfig();
        }
    }
    
    /**
     * This parses the HTML template to
     * - get the list of needed parameters (${xxx} parameters in the template, so get the foo, bar etc variables)
     * - create a usable template using the MessageFormat system (have strings with ${0}, ${1} instead of ${foo}, ${bar})
     * @param htmlTemplate
     */
    private void prepareTemplateAndParameters(String htmlTemplate)
    {   
        Pattern pattern = Pattern.compile("\\$\\{(.+?)}"); // Detect ${...} sequences
        Matcher matcher = pattern.matcher(htmlTemplate);
        
        // To construct a new template with ${0}, ${1} fields ..
        StringBuilder buffer = new StringBuilder();
        int previousStart = 0;
        int currentParameterIndex = 0;
        
        // Loop through the parameters in the template
        while (matcher.find()) {
            String parameter = matcher.group(1);

            String stringBeforeCurrentParam = htmlTemplate.substring(previousStart, matcher.start());
            
            // Add string before parameter
            buffer.append(stringBeforeCurrentParam);
            // Replace parameter with parameter index
            buffer.append("{" + currentParameterIndex + "}");
            
            // Save current parameter name in the list
            parameterNames.add(parameter);
            
            previousStart = matcher.end();
            currentParameterIndex++;
        }
        
        // Add string after the last parameter
        String stringAfterLastParam = htmlTemplate.substring(previousStart);
        buffer.append(stringAfterLastParam);
        
        // Create the HTML form
        htmlForm = new MessageFormat(buffer.toString());
    }
    
    /**
     * Parses content of the MAIL_TO configuration property
     */
    private String[] parseMailTo(String mailToStr)
    {
        String[] localMaiTos = mailToStr.split(",");
        String[] result = new String[localMaiTos.length];
        for (int i=0 ; i<localMaiTos.length ; i++)
        {
            result[i] =  localMaiTos[i].trim();
        }
        return result;
    }
    
    /**
     * Displays processor configuration
     */
    private void displayConfig()
    {
        StringBuilder sb = new StringBuilder("SendMail Processor configuration:");
        sb.append("\n" + SMTP_SERVER.getName() + ": " + smtpServer);
        sb.append("\n" + SMTP_PORT.getName() + ": " + smtpPort);
        sb.append("\n" + SMTP_SECURITY_USERNAME.getName() + ": " + smtpSecurityUsername);
        sb.append("\n" + SMTP_SECURITY_PASSWORD.getName() + ": " + smtpSecurityPassword);
        sb.append("\n" + SMTP_SECURITY_SSL.getName() + ": " + smtpSecuritySsl);
        sb.append("\n" + MAIL_FROM_ADDRESS.getName() + ": " + mailFromAddress);
        sb.append("\n" + MAIL_FROM_NAME.getName() + ": " + mailFromName);
        sb.append("\n" + MAIL_BOUNCE_ADDRESS.getName() + ": " + mailBounceAddress);
        sb.append("\n" + MAIL_REPLYTO_ADDRESS.getName() + ": " + mailReplyToAddress);
        sb.append("\n" + MAIL_SUBJECT.getName() + ": " + mailSubject);
        sb.append("\n" + MAIL_TO.getName() + ":");
        for (String mailTo : mailTos)
        {
            sb.append(" " + mailTo);
        }
        sb.append("\n" + ALLOW_OVERWRITE.getName() + ": " + allowFieldsOverwriting);
        sb.append("\n" + HTML_TEMPLATE.getName() + ": " + htmlTemplate);
        logger.info(sb.toString());
    }
}
