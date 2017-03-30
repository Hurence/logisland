
import JobsListSimple from 'src/jobs/components/list/JobsListSimple';
import JobController from 'src/job-list/job-list.component';
import JobList from 'src/job-list/job-list.component';
import JobDetails from 'src/job-list/job-details.component';
import JobDataService from 'src/job-list/job-list.service';

import StreamDetails from 'src/jobs/components/details/StreamDetails';
import EditProcessor from 'src/jobs/components/details/EditProcessor';
import EditRow from 'src/jobs/components/details/EditRow';
import Config from 'src/jobs/components/details/Config';

import ProcessorList from 'src/processor-list/processor-list.component';
import ProcessorDataService from 'src/processor-list/processor-list.service';
import ListService from 'src/jobs/services/ListService';
import AppSettings from 'src/jobs/services/AppSettings';



// Define the Angular 'jobs' module
export default angular.module('jobs', ['ngMaterial', 'ngResource', 'xeditable' ])

  .filter('cut', function () {
            return function (value, wordwise, max, tail) {
                if (!value) return '';

                max = parseInt(max, 10);
                if (!max) return value;
                if (value.length <= max) return value;

                value = value.substr(0, max);
                if (wordwise) {
                    var lastspace = value.lastIndexOf(' ');
                    if (lastspace !== -1) {
                      //Also remove . and , so its gives a cleaner result.
                      if (value.charAt(lastspace-1) === '.' || value.charAt(lastspace-1) === ',') {
                        lastspace = lastspace - 1;
                      }
                      value = value.substr(0, lastspace);
                    }
                }

                return value + (tail || ' …');
            };
        })

  .component(JobsListSimple.name, JobsListSimple.config)
  .component(JobList.name, JobList.config)
  .component(ProcessorList.name, ProcessorList.config)
  .component(JobDetails.name, JobDetails.config)
  .component(StreamDetails.name, StreamDetails.config)
  .component(EditProcessor.name, EditProcessor.config)
  .component(EditRow.name, EditRow.config)
  .component(Config.name, Config.config)

  .service("JobDataService", JobDataService)
  .service("ProcessorDataService", ProcessorDataService)
  .service("ListService", ListService)

  .factory("AppSettings", AppSettings)

  .controller('JobController', JobController)
  ;

