
export default {
    name: 'topicsList',
    config: {
        bindings: { topics: '<', mode: '<', selected: '<', showDetails: '&onSelected' },
        templateUrl: 'src/topics/components/list/TopicsList.html',
        controller: ['TopicsDataService', '$log', '$scope', TopicsListController]
    }
};

function TopicsListController(TopicsDataService, $log, $scope) {
    var vm = $scope;
    var self = this;

    self.selectedTopic = null;

    vm.addTopic = addTopic;
    vm.deleteTopic = deleteTopic;
    vm.selectTopic = selectTopic;

    vm.serializers = [
        { id: 'none', name: 'none' },
        { id: 'com.hurence.logisland.serializer.AvroSerializer', name: 'avro' },
        { id: 'com.hurence.logisland.serializer.KryoSerializer', name: 'kryo' }
    ];
    vm.selectedSerializer = { id: 'com.hurence.logisland.serializer.KryoSerializer', name: 'kryo' };

    function addTopic() {
        self.topics[self.topics.length] = DEFAULT_TOPIC_VALUES;
        self.selectedTopic = self.topics[self.topics.length - 1];
    }

    function deleteTopic(index) {
        self.topics.splice(index, 1);
    }

    function selectTopic(topic) {
        //$log.debug("topic selected: " + JSON.stringify(topic));
        self.selectedTopic = topic;
    }

    var DEFAULT_TOPIC_VALUES = {
        name: "topicName",
        partitions: 1,
        replicationFactor: 1,
        documentation: "doc",
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
}