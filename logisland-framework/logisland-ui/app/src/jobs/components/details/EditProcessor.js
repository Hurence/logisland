
export default {
  name : 'editProcessor',
  config : {
    bindings         : { processor: '<' },
    templateUrl      : 'src/jobs/components/details/EditProcessor.html',
    controller       : [ '$log', '$scope', EditProcessorController ]
  }
};

function EditProcessorController($log, $scope)  {
    var self = this;
};
