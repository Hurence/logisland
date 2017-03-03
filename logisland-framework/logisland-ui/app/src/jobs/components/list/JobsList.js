
export default {
  name : 'jobsList',
  config : {
    bindings         : { jobs: '<', mode: '<', selected : '<', showDetails : '&onSelected' },
    templateUrl      : 'src/jobs/components/list/JobsList.html',
    controller       : [ 'JobsDataService', '$log', '$scope', JobsListController ]
  }
};

function JobsListController(JobsDataService, $log, $scope, $templateCache) {
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


//    vm.table_options = {
//        columns: [
//            {
//                name: 'Name',
//                prop: 'name',
//                sort: 'asc',
//            },
//            {
//                name: 'Status',
//                prop: 'summary.status'
//            },
//            {
//                name: 'Actions',
//                cellRenderer: function(scope, elem, row) {
//                    return $templateCache.get('actions.html');
//                }
//            }
//        ],
//        scrollbarV: false,
//        forceFillColumns: true,
//        columnMode: 'force',
//        paging: {
//            externalPaging: false
//        }
//    };

    function editJob(job) {

    }

    function startJob(index) {
        var job = vm.$ctrl.jobs[index];
        JobsDataService.start({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.$ctrl.jobs[index] = updatedJob;
        });
    }

    function restartJob(index) {
        var job = vm.$ctrl.jobs[index];
        JobsDataService.restart({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.$ctrl.jobs[index] = updatedJob;
        });
    }

    function pauseJob(index) {
        var job = vm.$ctrl.jobs[index];
        JobsDataService.pause({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.$ctrl.jobs[index] = updatedJob;
        });
    }

    function shutdownJob(index) {
        var job = vm.$ctrl.jobs[index];
        JobsDataService.shutdown({id: job.name}, function(updatedJob, getResponseHeaders) {
            vm.$ctrl.jobs[index] = updatedJob;
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