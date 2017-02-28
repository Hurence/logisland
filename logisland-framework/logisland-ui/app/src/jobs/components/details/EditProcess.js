
export default {
  name : 'editProcess',
  config : {
    bindings         : { processor: '<' },
    templateUrl      : 'src/jobs/components/details/EditProcess.html',
    controller       : [ '$log', '$scope', EditProcessController ]
  }
};

function EditProcessController($log, $scope)  {
    var vm = $scope;

    vm.processor = null;
    vm.done = done;

    vm.$on('editProcessor', function(event, processor) {
        if(vm.processor == processor) {
            done();
        }
        else {
            $log.debug("Editing processor: " + processor.name);
            vm.processor = processor;
        }
    });

    function done() {
        vm.processor = null;
    }
};
