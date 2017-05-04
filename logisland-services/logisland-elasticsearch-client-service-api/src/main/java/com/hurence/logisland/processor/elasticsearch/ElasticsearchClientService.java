package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.controller.ControllerService;


/*import com.hurence.logisland.processor.hbase.put.PutColumn;
import com.hurence.logisland.processor.hbase.put.PutRecord;
import com.hurence.logisland.processor.hbase.scan.Column;
import com.hurence.logisland.processor.hbase.scan.ResultHandler;
import com.hurence.logisland.processor.hbase.validate.ConfigFilesValidator;
*/

@Tags({"elasticsearch", "client"})
@CapabilityDescription("A controller service for accessing an elasticsearch client.")
public interface ElasticsearchClientService extends ControllerService {



}
