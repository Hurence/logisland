
export default {
  name : 'jobDetails',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/job-list/job-details.template.html',
    controller       : [ 'JobDataService', '$log', '$scope', '$mdDialog', JobDetailsController ]
  }
};

function JobDetailsController(JobDataService, $log, $scope, $mdDialog)  {
    var vm = $scope;
var self = this;
    vm.editingJobName = false;
    vm.previousJobName = null;

    vm.startEditingJobName = startEditingJobName;
    vm.doneEditingJobName = doneEditingJobName;


    vm.addStream    = addStream;
    vm.deleteStream = deleteStream;
    vm.edit         = edit;
    vm.doneEditing  = doneEditing;


    vm.$watch("$ctrl.selected", function(newJob, oldJob) {
        if(newJob) {
            if (newJob.streams.length > 0) {
                vm.$ctrl.selectedStream = newJob.streams[0];
            }
        }
    });

    function startEditingJobName(job) {
        vm.previousJobName = job.name;
        vm.editingJobName = true;
    }

    function doneEditingJobName(job) {
        vm.editingJobName = false;
        if(vm.previousJobName !== job.name) {
            JobDataService.save(job);
            JobDataService.delete({id: vm.previousJobName});
            vm.previousJobName = null;
        }
    }

    function deleteJob(job) {
        var confirm = $mdDialog.confirm()
            .title('Do you confirm the deletion?')
            .textContent('This will delete the selected job permanently.')
            .ariaLabel('Delete job')
            //.targetEvent(ev)
            .ok('Delete!')
            .cancel('Cancel');
        $mdDialog.show(confirm)
        .then(
            function() {
                JobDataService.delete({id: job.name});
            },
            function() {
            }
        );
    }

    function edit(item) {
        item.editing = true;
    }

    function doneEditing(item) {
        item.editing = false;
    }

    function addStream() {
        vm.$ctrl.selected.streams.push({"name": "[Stream name]", "component": "comp1", "config": [], "processors": []});
        vm.$ctrl.selectedStream = vm.$ctrl.selected.streams[vm.$ctrl.selected.streams.length-1];
    }

    function deleteStream() {
        if(vm.$ctrl.selectedStream) {
            var index = vm.$ctrl.selected.streams.indexOf(vm.$ctrl.selectedStream);
            if(index >= 0) {
                vm.$ctrl.selected.streams.splice(index, 1);
            }
        }
    }

  

   
};
