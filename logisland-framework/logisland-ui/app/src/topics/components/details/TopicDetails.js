
export default {
  name : 'topicDetails',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/topics/components/details/TopicDetails.html',
    controller       : [ 'TopicsDataService', '$log', '$scope', TopicDetailsController ]
  }
};

function TopicDetailsController(TopicsDataService, $log, $scope)  {
    var vm = $scope;
    var self = this;

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
};
