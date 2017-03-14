
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

};
