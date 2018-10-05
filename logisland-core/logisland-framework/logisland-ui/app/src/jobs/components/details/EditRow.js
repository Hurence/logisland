export default {
  name : 'editRow',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/jobs/components/details/EditRow.html',
    controller       : [ '$log', '$scope', EditRowController ]
  }
};

function EditRowController($log, $scope, row) {
    var vm = $scope;

    vm.done = done;

    function done() {
        $log.debug("done editing row");
    }
}
