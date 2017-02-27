
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

    vm.saveJob = saveJob;

    function saveJob(job) {
        JobsDataService.save(job);
    }
};
