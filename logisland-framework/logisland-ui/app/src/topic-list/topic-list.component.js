
export default {
    name: 'topicsList',
    config: {
        bindings: { topics: '<', mode: '<', selected: '<', showDetails: '&onSelected' },
        templateUrl: 'src/topic-list/topic-list.template.html',
        controller: ['TopicsDataService', '$log', '$scope', '$timeout', TopicsListController]
    }
};

function TopicsListController(TopicsDataService, $log, $scope, $timeout) {
    var vm = $scope;
    var self = this;

    self.selectedTopic = null;
    self.topics = TopicsDataService.query(function () { (self.topics.length > 0) ? self.selectedTopic = self.topics[0] : self.selectedTopic = null; });
    self.filteredTopics = self.topics;

    self.simulateQuery = false;
    self.isDisabled    = false;

    self.querySearch   = querySearch;
    self.selectedItemChange = selectedItemChange;
    self.searchTextChange   = searchTextChange;

    self.pressEnter = pressEnter;

    // ******************************
    // Internal methods
    // ******************************

    /**
     * Search for repos... use $timeout to simulate
     * remote dataservice call.
     */
    function querySearch (query) {

        self.filteredTopics = query
            ? self.topics.filter(createFilterFor(query))
            : self.topics;


            return self.filteredTopics;


    }


function pressEnter(){
    var autoChild = document.getElementById('Auto').firstElementChild;
    var el = angular.element(autoChild).blur();
}
    function searchTextChange(text) {
      $log.info('Text changed to ' + text);
    }

    function selectedItemChange(item) {
        if(item != undefined){
 $log.info('Item changed to ' + JSON.stringify(item));
      querySearch(item.name);
        }
     
    }

   

    /**
     * Create filter function for a query string
     */
    function createFilterFor(query) {
      var lowercaseQuery = angular.lowercase(query);

      return function filterFn(item) {
        return (item.name.search(lowercaseQuery) != -1) || (item.documentation.search(lowercaseQuery) != -1);
      };

    }


    self.selectedTopic = null;

    vm.formatDate = formatDate;
    vm.addTopic = addTopic;
    vm.deleteTopic = deleteTopic;
    vm.selectTopic = selectTopic;

    vm.serializers = [
        { id: 'none', name: 'none' },
        { id: 'com.hurence.logisland.serializer.AvroSerializer', name: 'avro' },
        { id: 'com.hurence.logisland.serializer.KryoSerializer', name: 'kryo' }
    ];
    vm.selectedSerializer = { id: 'com.hurence.logisland.serializer.KryoSerializer', name: 'kryo' };

     vm.formatDate = formatDate;
    vm.saveTopic  = saveTopic;
    vm.convertToJson = convertToJson;
    vm.convertToString = convertToString;

    $log.debug("Topic selected");

    function formatDate(instant) {
        return new Date(instant);
    }

    function saveTopic(topic) {
        $log.debug("Saving topic");
        TopicsDataService.save(topic);
    }

    function convertToJson(val) {
        val = JSON.parse(val);
        return val
    }

    function convertToString(val) {
        return JSON.stringify(val);
    }

    function addTopic() {
        self.topics[self.topics.length] = DEFAULT_TOPIC_VALUES;
        self.selectedTopic = self.topics[self.topics.length - 1];
    }

    function deleteTopic(index) {
        self.topics.splice(index, 1);
    }

    function selectTopic(topic) {
        $log.debug("topic selected: " + JSON.stringify(topic));
        self.selectedTopic = topic;
    }

    var DEFAULT_TOPIC_VALUES = {
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
}