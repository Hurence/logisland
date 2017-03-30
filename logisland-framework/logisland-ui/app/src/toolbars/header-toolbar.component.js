
export default {
  name : 'headerToolbar',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/toolbars/header-toolbar.template.html',
    controller       : [ 'JobDataService', 'TopicsDataService', '$log', '$scope', HeaderToolbarController ]
  }
};

function HeaderToolbarController(JobDataService, TopicsDataService, $log, $scope)  {
    var vm = $scope;
    var self = this;

    self.addTopic = addTopic;
    self.addJob = addJob;

    var DEFAULT_TOPIC = {
        name: "topicName",
        partitions: 1,
        replicationFactor: 1,
        documentation: "describe here the content of the topic",
        serializer: "com.hurence.logisland.serializer.KryoSerializer",
        businessTimeField: "record_time",
        rowkeyField: "record_id",
        recordTypeField: "record_type",
        keySchema: [
            { name: "key1", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" },
            { name: "key2", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" }
        ],
        valueSchema: [
            { name: "value1", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" },
            { name: "value2", encrypted: false, indexed: true, persistent: true, optional: true, type: "STRING" }
        ]
    };

    var DEFAULT_JOB =  { 
        name: "newJobTemplate", 
        streams: [{ "name": "[Stream name]", "component": "comp1", "config": [], "processors": [] }] 
    };

    function addTopic() {
        TopicsDataService.save(DEFAULT_TOPIC);
    }

    function addJob() {
        JobDataService.save(DEFAULT_JOB);
    }
};
