
export default {
    name: 'jobList',
    config: {
        bindings: { jobs: '<', mode: '<', selected: '<', showDetails: '&onSelected' },
        templateUrl: 'src/job-list/job-list.template.html',
        controller: ['JobDataService', '$log', '$scope', '$timeout', '$mdDialog', JobListController]
    }
};

function JobListController(JobDataService, $log, $scope, $timeout, $mdDialog) {
    var vm = $scope;
    var self = this;

    self.selectedjob = null;
    self.jobs = null;
    refreshJobs();
    self.filteredJobs = null;


    self.newJobTemplate = { name: "newJobTemplate", streams: [{ "name": "[Stream name]", "component": "comp1", "config": [], "processors": [] }] };



    self.simulateQuery = false;
    self.isDisabled = false;

    self.querySearch = querySearch;
    self.selectedItemChange = selectedItemChange;
    self.searchTextChange = searchTextChange;
    self.searchText = '';
    self.pressEnter = pressEnter;
    self.refreshJobs = refreshJobs;

    vm.formatDate = formatDate;
    vm.deleteJob = deleteJob;
    vm.formatDate = formatDate;
    vm.saveJob = saveJob;
    vm.convertToJson = convertToJson;
    vm.convertToString = convertToString;
    vm.deleteJob    = deleteJob;
    vm.startJob     = startJob;
    vm.shutdownJob  = shutdownJob;
    vm.pauseJob     = pauseJob;

    function refreshJobs() {
        JobDataService.query(function (result) {
            if (result.length > 0) {
                self.jobs = result;
                self.filteredJobs = querySearch(self.searchText);
                $log.debug(self.filteredJobs);
            }
        });
    }


    // ******************************
    // Internal methods
    // ******************************

    /**
     * Search for repos... use $timeout to simulate
     * remote dataservice call.
     */
    function querySearch(query) {
        self.searchText = query;
        self.filteredJobs = query
            ? self.jobs.filter(createFilterFor(query))
            : self.jobs;

        return self.filteredJobs;
    }

    /**
     * Create filter function for a query string
     */
    function createFilterFor(query) {
        var lowercaseQuery = angular.lowercase(query);

        return function filterFn(item) {
            return (angular.lowercase(item.name).search(lowercaseQuery) != -1) ||
                (angular.lowercase(item.summary.documentation).search(lowercaseQuery) != -1);
        };

    }

    function pressEnter() {
        var autoChild = document.getElementById('Auto').firstElementChild;
        var el = angular.element(autoChild).blur();
        querySearch(self.searchText);
    }
    function searchTextChange(text) {
        querySearch(text);
    }

    function selectedItemChange(item) {
        if (item != undefined) {
            $log.info('Item changed to ' + JSON.stringify(item));
            querySearch(item.name);
        }

    }




    function formatDate(instant) {
        return new Date(instant);
    }

    function convertToJson(val) {
        val = JSON.parse(val);
        return val
    }

    function convertToString(val) {
        return JSON.stringify(val);
    }

    // ******************************
    // job actions
    // ******************************

    function saveJob(job) {
        $log.debug("Saving job");
        JobDataService.save(job, function () {
            self.jobs = refreshJobs();
        });

    }

    function deleteJob(ev, job) {

        // Appending dialog to document.body to cover sidenav in docs app
        var confirm = $mdDialog.confirm()
            .title('Would you like to delete job ' + job.name + '?')
            .textContent('The job will be definitively removed.')
            .ariaLabel('confirm delete')
            .targetEvent(ev)
            .ok('Delete')
            .cancel('cancel');

        $mdDialog.show(confirm).then(function () {
            $log.debug("Deleting job " + job.name);
            JobDataService.delete({ id: job.name }, function () {
                $log.debug("deleted");
                self.searchText = '';
                self.jobs = refreshJobs();
            });
        }, function () {
            $scope.status = 'You decided to keep your debt.';
        });



        //      self.jobs.splice(index, 1);
    }

 function startJob(job) {
        JobDataService.start({id: job.name}, function(updatedJob, getResponseHeaders) {
            self.jobs = refreshJobs();
        });
    }

    function restartJob(job) {
        JobDataService.restart({id: job.name}, function(updatedJob, getResponseHeaders) {
             self.jobs = refreshJobs();
        });
    }

    function pauseJob(job) {
        JobDataService.pause({id: job.name}, function(updatedJob, getResponseHeaders) {
             self.jobs = refreshJobs();
        });
    }

    function shutdownJob(job) {
        JobDataService.shutdown({id: job.name}, function(updatedJob, getResponseHeaders) {
            self.jobs = refreshJobs();
        });
    }

}