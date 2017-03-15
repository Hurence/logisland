
export default {
  name : 'topicsList',
  config : {
    bindings         : { topics: '<', mode: '<', selected : '<', showDetails : '&onSelected' },
    templateUrl      : 'src/topics/components/list/TopicsList.html',
    controller       : [ 'TopicsDataService', '$log', '$scope', TopicsListController ]
  }
};

function TopicsListController(TopicsDataService, $log, $scope) {
    var vm = $scope;
    var self = this;

    self.selectedTopic = null;

    vm.selectTopic = selectTopic;

    function selectTopic(topic) {
        //$log.debug("topic selected: " + JSON.stringify(topic));
        self.selectedTopic = topic;
    }
}