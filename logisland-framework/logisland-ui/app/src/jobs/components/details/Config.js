
export default {
  name : 'config',
  config : {
    bindings         : { configuration: '<' },
    templateUrl      : 'src/jobs/components/details/Config.html',
    controller       : [ '$log', '$scope', ConfigController ]
  }
};

function ConfigController($log, $scope)  {
    var vm = $scope;
};
