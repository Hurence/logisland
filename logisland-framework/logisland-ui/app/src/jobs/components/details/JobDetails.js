
export default {
  name : 'jobDetails',
  config : {
    bindings         : { selected: '<' },
    templateUrl      : 'src/jobs/components/details/JobDetails.html',
    controller       : [ 'JobsDataService', '$log', '$scope', '$mdDialog', JobDetailsController ]
  }
};

function JobDetailsController(JobsDataService, $log, $scope, $mdDialog)  {
    var vm = $scope;

    vm.editingJobName = false;
    vm.previousJobName = null;

    vm.startEditingJobName = startEditingJobName;
    vm.doneEditingJobName = doneEditingJobName;
    vm.deleteJob    = deleteJob;
    vm.startJob     = startJob;
    vm.shutdownJob  = shutdownJob;
    vm.pauseJob     = pauseJob;

    vm.addStream    = addStream;
    vm.deleteStream = deleteStream;
    vm.edit         = edit;
    vm.doneEditing  = doneEditing;
    vm.saveJob      = saveJob;

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
            JobsDataService.save(job);
            JobsDataService.delete({id: vm.previousJobName});
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
                JobsDataService.delete({id: job.name});
            },
            function() {
            }
        );
    }

    function edit(item) {
        item.editing = true;
    }

    function doneEditing(stream) {
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

    function saveJob(job) {
        $log.debug(job.streams);
        JobsDataService.save(job);
    }

    function startJob(job) {
        JobsDataService.start({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.selected = updatedJob;
        });
    }

    function restartJob(job) {
        JobsDataService.restart({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.selected = updatedJob;
        });
    }

    function pauseJob(job) {
        JobsDataService.pause({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.selected = updatedJob;
        });
    }

    function shutdownJob(job) {
        JobsDataService.shutdown({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.selected = updatedJob;
        });
    }
};
