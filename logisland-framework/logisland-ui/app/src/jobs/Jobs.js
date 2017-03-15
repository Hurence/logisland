
import JobsListSimple from 'src/jobs/components/list/JobsListSimple';
import JobsList from 'src/jobs/components/list/JobsList';
import ProcessorsList from 'src/jobs/components/list/ProcessorsList';
import JobDetails from 'src/jobs/components/details/JobDetails';
import TopicDetails from 'src/topics/components/details/TopicDetails';
import StreamDetails from 'src/jobs/components/details/StreamDetails';
import EditProcessor from 'src/jobs/components/details/EditProcessor';
import EditRow from 'src/jobs/components/details/EditRow';
import Config from 'src/jobs/components/details/Config';

import JobsDataService from 'src/jobs/services/JobsDataService';
import ProcessorsDataService from 'src/jobs/services/ProcessorsDataService';
import ListService from 'src/jobs/services/ListService';
import AppSettings from 'src/jobs/services/AppSettings';

import JobsController from 'src/jobs/JobsController';

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
  .component(JobsList.name, JobsList.config)
  .component(ProcessorsList.name, ProcessorsList.config)
  .component(JobDetails.name, JobDetails.config)
  .component(TopicDetails.name, TopicDetails.config)
  .component(StreamDetails.name, StreamDetails.config)
  .component(EditProcessor.name, EditProcessor.config)
  .component(EditRow.name, EditRow.config)
  .component(Config.name, Config.config)

  .service("JobsDataService", JobsDataService)
  .service("ProcessorsDataService", ProcessorsDataService)
  .service("ListService", ListService)

  .factory("AppSettings", AppSettings)

  .controller('JobsController', JobsController)
  ;

