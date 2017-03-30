
export default {
  name : 'jobsList',
  config : {
    bindings         : { jobs: '<', mode: '<', selected : '<', showDetails : '&onSelected' },
    templateUrl      : 'src/jobs/components/list/JobsList.html',
    controller       : [ 'JobDataService', '$log', '$scope', JobsListController ]
  }
};

function JobsListController(JobDataService, $log, $scope) {
    var vm = $scope;
    var self = this;

    self.filteredJobs = vm.$ctrl.jobs;

    vm.editJob = editJob;
    vm.startJob = startJob;
    vm.pauseJob = pauseJob;
    vm.shutdownJob = shutdownJob;
    vm.restartJob = restartJob;

    vm.querySearch        = querySearch;
    vm.selectedItemChange = selectedItemChange;
    vm.searchTextChange   = searchTextChange;

    function editJob(job) {
    }

    function startJob(index) {
        var job = self.filteredJobs[index];
        JobDataService.start({id: job.name}, function(updatedJob, getResponseHeaders) {
            self.filteredJobs[index] = updatedJob;
        });
    }

    function restartJob(index) {
        var job = self.filteredJobs[index];
        JobDataService.restart({id: job.name}, function(updatedJob, getResponseHeaders) {
            self.filteredJobs[index] = updatedJob;
        });
    }

    function pauseJob(index) {
        var job = self.filteredJobs[index];
        JobDataService.pause({id: job.name}, function(updatedJob, getResponseHeaders) {
            self.filteredJobs[index] = updatedJob;
        });
    }

    function shutdownJob(index) {
        var job = self.filteredJobs[index];
        JobDataService.shutdown({id: job.name}, function(updatedJob, getResponseHeaders) {
            self.filteredJobs[index] = updatedJob;
        });
    }

    function querySearch (query) {
        var results = query
                ? self.jobs.filter( createFilterFor(query) )
                : self.jobs;
        this.filteredJobs = results;
        return results;
    }

    function createFilterFor(query) {
        var lowercaseQuery = angular.lowercase(query);

        return function filterFn(job) {
            return (job.name.indexOf(lowercaseQuery) === 0);
        };
    }

    function searchTextChange(text) {
          $log.info('Text changed to ' + text);
    }

    function selectedItemChange(job) {
        selectJob(job);
    }
}