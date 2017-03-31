
import TopicsDataService from 'src/topic-list/topic-list.service';
import ListService from 'src/jobs/services/ListService';
import AppSettings from 'src/app.service';

import TopicsList from 'src/topic-list/topic-list.component';
import KeyTypesList from 'src/fieldtype-list/fieldtype-list.component';

// Define the Angular 'topics' module
export default angular
  .module("topics", ['ngMaterial', 'ngResource', 'xeditable'])

  .component(TopicsList.name, TopicsList.config)
  .component(KeyTypesList.name, KeyTypesList.config)

  .service("TopicsDataService", TopicsDataService)
  .service("ListService", ListService)

  .factory("AppSettings", AppSettings)
  ;

