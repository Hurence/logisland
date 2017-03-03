
import JobsListSimple from 'src/jobs/components/list/JobsListSimple';
import JobsList from 'src/jobs/components/list/JobsList';
import ProcessorsList from 'src/jobs/components/list/ProcessorsList';
import JobDetails from 'src/jobs/components/details/JobDetails';
import StreamDetails from 'src/jobs/components/details/StreamDetails';
import EditProcessor from 'src/jobs/components/details/EditProcessor';
import EditRow from 'src/jobs/components/details/EditRow';
import Config from 'src/jobs/components/details/Config';
//import TopicSchema from 'src/jobs/components/details/TopicSchema';

import JobsDataService from 'src/jobs/services/JobsDataService';
import ProcessorsDataService from 'src/jobs/services/ProcessorsDataService';
import ListService from 'src/jobs/services/ListService';
import AppSettings from 'src/jobs/services/AppSettings';


// Define the Angular 'jobs' module
export default angular
  .module("jobs", ['ngMaterial', 'ngResource', 'data-table' ])

  .component(JobsListSimple.name, JobsListSimple.config)
  .component(JobsList.name, JobsList.config)
  .component(ProcessorsList.name, ProcessorsList.config)
  .component(JobDetails.name, JobDetails.config)
  .component(StreamDetails.name, StreamDetails.config)
  .component(EditProcessor.name, EditProcessor.config)
  .component(EditRow.name, EditRow.config)
  .component(Config.name, Config.config)
  //.component(TopicSchema.name, TopicSchema.config)

  .service("JobsDataService", JobsDataService)
  .service("ProcessorsDataService", ProcessorsDataService)
  .service("ListService", ListService)

  .factory("AppSettings", AppSettings)
  ;

