import JobsList from 'src/jobs/components/list/JobsList';
import ProcessorsList from 'src/jobs/components/list/ProcessorsList';
import JobDetails from 'src/jobs/components/details/JobDetails';
import StreamDetails from 'src/jobs/components/details/StreamDetails';
import EditProcess from 'src/jobs/components/details/EditProcess';
import Config from 'src/jobs/components/details/Config';
//import TopicSchema from 'src/jobs/components/details/TopicSchema';

import JobsDataService from 'src/jobs/services/JobsDataService';
import ProcessorsDataService from 'src/jobs/services/ProcessorsDataService';
import ListService from 'src/jobs/services/ListService';
import AppSettings from 'src/jobs/services/AppSettings';


// Define the Angular 'jobs' module
export default angular
  .module("jobs", ['ngMaterial', 'ngResource', 'ui.grid'])

  .component(JobsList.name, JobsList.config)
  .component(ProcessorsList.name, ProcessorsList.config)
  .component(JobDetails.name, JobDetails.config)
  .component(StreamDetails.name, StreamDetails.config)
  .component(EditProcess.name, EditProcess.config)
  .component(Config.name, Config.config)
  //.component(TopicSchema.name, TopicSchema.config)

  .service("JobsDataService", JobsDataService)
  .service("ProcessorsDataService", ProcessorsDataService)
  .service("ListService", ListService)

  .factory("AppSettings", AppSettings)
  ;

