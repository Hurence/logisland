
export default {
  name : 'jobDetails',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/jobs/components/details/JobDetails.html',
    controller       : [ 'JobsDataService', '$log', '$scope', JobDetailsController ]
  }
};

function JobDetailsController(JobsDataService, $log, $scope)  {
    var vm = $scope;

    vm.addStream    = addStream;
    vm.deleteStream = deleteStream;
    vm.edit         = edit;
    vm.doneEditing  = doneEditing;
    vm.saveJob      = saveJob;

    function edit(stream) {
        stream.editing = true;
    }

    function doneEditing(stream) {
        stream.editing = false;
    }

    function addStream() {
        vm.$ctrl.selected.streams.push({"name": "[Stream name]", "component": "comp1", "config": [], "processors": []});
    }

    function deleteStream() {
        if(vm.$ctrl.selectedStream) {
            var index = vm.$ctrl.selected.streams.indexOf(vm.$ctrl.selectedStream);
            if(index >= 0) {
                vm.$ctrl.selected.streams.splice(index, 1);
            }
        }
    }

    function saveJob(job) {
        $log.debug(job.streams);
        JobsDataService.save(job);
    }
};
