/**
 * Copyright (C) 2017 Hurence
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.mailer;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.*;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.string.JsonUtil;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import org.apache.commons.mail.SimpleEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mailer Processor
 */
@Tags({"smtp", "email", "e-mail", "mail", "mailer", "message", "alert"})
@CapabilityDescription(
        "The Mailer processor is aimed at sending an email (like for instance an alert email) from an incoming record."
        + " To generate an email and trigger an email sending, an incoming record must have a mail_msg field with the content of the mail as value."
        + " Other optional mail_* fields may be used to customize the Mailer processor upon reception of the record.")
public class MailerProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(MailerProcessor.class);
    
    // Easy trick to not allow debugging without changing the logger level but instead using a configuration key
    private boolean debug = false;

    private String smtpServer = null;
    private int smtpPort = 25;
    private String[] mailTos = new String[]{};
    private String mailFromAddress = null;
    private String mailFromName = null;
    private String mailSubject = null;
    private boolean allowFieldsOverwriting = true;

    /**
     * Definitions for the fields of the incoming record
     */
    
    private static final String FIELD_PREFIX = "mail";
    
    // Sole mandatory field. This holds the content of the mail to be sent.
    public static final String FIELD_MAIL_MSG = FIELD_PREFIX + "_msg";

    // May be used to overwrite mail.to configured in processor
    public static final String FIELD_MAIL_TO = FIELD_PREFIX + "_to";
    // May be used to overwrite mail.from.address configured in processor 
    public static final String FIELD_MAIL_FROM_ADDRESS = FIELD_PREFIX + "_from_address";    
    // May be used to overwrite mail.from.name configured in processor 
    public static final String FIELD_MAIL_FROM_NAME = FIELD_PREFIX + "_from_name";
    // May be used to overwrite mail.subject configured in processor 
    public static final String FIELD_MAIL_SUBJECT = FIELD_PREFIX + "_subject";
    
    /**
     * Configuration keys
     */
    
    // Easy trick to not allow debugging without changing the logger level but instead using a configuration key
    private static final String KEY_DEBUG = "debug";

    private static final String KEY_SMTP_SERVER = "smtp.server";
    private static final String KEY_SMTP_PORT = "smtp.port";

    private static final String KEY_MAIL_TO = "mail.to";
    private static final String KEY_MAIL_FROM_ADDRESS = "mail.from.address";
    private static final String KEY_MAIL_FROM_NAME = "mail.from.name";
    private static final String KEY_MAIL_SUBJECT = "mail.subject";
    
    private static final String KEY_ALLOW_OVERWRITE = "allow_overwrite";
    
    public static final PropertyDescriptor DEBUG = new PropertyDescriptor.Builder()
            .name(KEY_DEBUG)
            .description("Enable debug. If enabled, debug information are written into stdout.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();
    
    public static final PropertyDescriptor SMTP_SERVER = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_SERVER)
            .description("Hostname or IP address of the SMTP server to use.")
            .required(true)
            .build();
    
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name(KEY_SMTP_PORT)
            .description("TCP port number of the SMTP server to use.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("25")
            .required(false)
            .build();
    
    public static final PropertyDescriptor MAIL_FROM_ADDRESS = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_FROM_ADDRESS)
            .description("Mail sender email.")
            .required(false)
            .defaultValue("logisland@yourdomain.com")
            .build();
    
    public static final PropertyDescriptor MAIL_FROM_NAME = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_FROM_NAME)
            .description("Mail sender name.")
            .required(false)
            .defaultValue("Logisland")
            .build();
    
    public static final PropertyDescriptor MAIL_SUBJECT = new PropertyDescriptor.Builder()
            .name(KEY_MAIL_SUBJECT)
            .description("Mail subject.")
            .required(false)
            .defaultValue("[LOGISLAND] Automatic email.")
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
                    FIELD_MAIL_TO + ", " + FIELD_MAIL_FROM_ADDRESS + ", " + FIELD_MAIL_SUBJECT +" etc). If false, special record fields"
                    + " are ignored and only processor configuration keys are used.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    @Override
    public void init(final ProcessContext context)
    {
        logger.debug("Initializing Mailer Processor");
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DEBUG);
        descriptors.add(SMTP_SERVER);
        descriptors.add(SMTP_PORT);
        descriptors.add(MAIL_FROM_ADDRESS);
        descriptors.add(MAIL_FROM_NAME);
        descriptors.add(MAIL_SUBJECT);
        descriptors.add(MAIL_TO);
        descriptors.add(ALLOW_OVERWRITE);

        return Collections.unmodifiableList(descriptors);
    }
  
    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records)
    {
        if (debug)
        {
            logger.info("Mailer Processor records input: " + records);
        }

        /**
         * Transform the records into mails and send them
         */
        for (Record record : records)
        {            
            
            Field mailMsgField = record.getField(FIELD_MAIL_MSG);
            if (mailMsgField != null)
            {
                String mailMsg = mailMsgField.asString();
                if (mailMsg == null)
                {
                    continue;
                }
                
                // Ok, there is a mail_msg field, create the mail and send it
                
//                try {
//                    SimpleEmail email = new SimpleEmail();
//                    
//                    recuperer les champs par defaut du record pour overwriter si present
//                    
//                    email.setFrom(fromEmailAddress, fromEmailName);
//                    
//                    email.addTo(toEmailAddress);
//                    email.setSubject("Alert");
//                    email.setMsg(notificationMessage);
//                   
//                    email.setHostName(smtpServerAddress);
//                    email.setSmtpPort(smtpServerPort);
//                    email.send();
//                } catch (EmailException ex) {
//                    TODO
//                }
            }
        }

        if (debug)
        {
            logger.info("Mailer Processor records output: " + records);
        }
        return records;
    }
    
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        logger.debug("customValidate");

        /**
         * If the mail.to configuration key is not set, allow_overwrite should be true so that we know to who we must
         * send the mail (record must then hold a mail_to field)
         */
        if (!context.getPropertyValue(MAIL_TO).isSet())
        {
            if (context.getPropertyValue(ALLOW_OVERWRITE).isSet()) {
                validationResults.add(
                        new ValidationResult.Builder()
                            .explanation("If  " + MAIL_TO.getName() + " is not set,  " + ALLOW_OVERWRITE.getName() + " must be true"
                                    + " so that the record holds a " + FIELD_MAIL_TO + " field that is used")
                            .valid(false)
                            .build());
            }
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
        
        if (debug)
        {
            displayConfig();
        }
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
        StringBuilder sb = new StringBuilder("Mailer Processor configuration:");
        sb.append("\n" + SMTP_SERVER.getName() + ": " + smtpServer);
        sb.append("\n" + SMTP_PORT.getName() + ": " + smtpPort);
        sb.append("\n" + MAIL_FROM_ADDRESS.getName() + ": " + mailFromAddress);
        sb.append("\n" + MAIL_FROM_NAME.getName() + ": " + mailFromName);
        sb.append("\n" + MAIL_SUBJECT.getName() + ": " + mailSubject);
        sb.append("\n" + MAIL_TO.getName() + ":");
        for (String mailTo : mailTos)
        {
            sb.append(" " + mailTo);
        }
        sb.append("\n" + ALLOW_OVERWRITE.getName() + ": " + allowFieldsOverwriting);
        logger.info(sb.toString());
    }
}
