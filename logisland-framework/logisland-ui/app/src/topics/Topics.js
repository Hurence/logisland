
import TopicsDataService from 'src/topics/services/TopicsDataService';
import ListService from 'src/jobs/services/ListService';
import AppSettings from 'src/jobs/services/AppSettings';

import TopicsList from 'src/topics/components/list/TopicsList';
import KeyTypesList from 'src/topics/components/list/KeyTypesList';

// Define the Angular 'topics' module
export default angular
  .module("topics", ['ngMaterial', 'ngResource', 'xeditable'])

  .component(TopicsList.name, TopicsList.config)
  .component(KeyTypesList.name, KeyTypesList.config)

  .service("TopicsDataService", TopicsDataService)
  .service("ListService", ListService)

  .factory("AppSettings", AppSettings)
  ;

