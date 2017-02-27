/**
 * Job App Controller for the LogIsland UI
 */

export default [ 'JobsDataService', 'ProcessorsDataService', 'ListService', 'AppSettings', '$mdSidenav', '$log', AppJobsController ];

function AppJobsController(JobsDataService, ProcessorsDataService, ListService, AppSettings, $mdSidenav, $log) {
  var self = this;

  self.version              = AppSettings.version;
  self.jobs                 = JobsDataService.query(function() { (self.jobs.length>0) ? self.selectedJob = self.jobs[0] : self.selectedJob = null   });
  self.selectedJob          = null;
  self.selectedProcessor    = null;

  self.toggleList       = ListService.toggle;
  self.selectJob        = selectJob;

  function selectJob ( job ) {
    self.selectedJob = angular.isNumber(job) ? self.jobs[job] : job;
  }
}