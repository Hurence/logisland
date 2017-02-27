// Load libraries
import angular from 'angular';

import 'angular-animate';
import 'angular-aria';
import 'angular-material';
import 'angular-messages';
import 'angular-resource';
import 'angular-ui-grid';

import AppJobsController from 'src/AppJobsController';
import Jobs from 'src/jobs/Jobs';

export default angular.module( 'app', [ 'ngMaterial', 'ngResource', 'ngMessages', Jobs.name ] )
  .config(($mdIconProvider, $mdThemingProvider) => {
    // Register the `avatar` icons
    $mdIconProvider
      .defaultIconSet("./assets/svg/avatars.svg", 128)
      .icon("add", "./assets/svg/round-add-button.svg")
      .icon("close", "./assets/svg/close-button.svg")
      .icon("delete", "./assets/svg/round-delete-button.svg")
      .icon("down", "./assets/svg/down-arrow.svg")
      .icon("ellipsis", "./assets/svg/ellipsis.svg.svg")
      .icon("menu", "./assets/svg/menu.svg", 24)
      .icon("save", "./assets/svg/save-button.svg")
      .icon("settings", "./assets/svg/settings-cogwheel-button.svg")
      .icon("up", "./assets/svg/up-arrow.svg");

//    $mdThemingProvider.theme('default')
//      .primaryPalette('brown')
//      .accentPalette('red');
  })
  .controller('AppController', AppJobsController)
  ;
