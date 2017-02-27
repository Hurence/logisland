// Load libraries
import angular from 'angular';

import 'angular-animate';
import 'angular-aria';
import 'angular-material';
import 'angular-messages';
import 'angular-resource';

import AppJobsController from 'src/AppJobsController';
import Jobs from 'src/jobs/Jobs';

export default angular.module( 'logisland-ui', [ 'ngMaterial', 'ngResource', 'ngMessages', Jobs.name ] )
  .config(($mdIconProvider, $mdThemingProvider) => {
    // Register the `avatar` icons
    $mdIconProvider
      .defaultIconSet("./assets/svg/avatars.svg", 128)
      .icon("menu", "./assets/svg/menu.svg", 24)
      .icon("job", "./assets/svg/job.png", 24);

    $mdThemingProvider.theme('default')
      .primaryPalette('brown')
      .accentPalette('red');
  })
  .controller('AppController', AppJobsController)
  ;
