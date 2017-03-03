// Load libraries
import angular from 'angular';

import 'angular-animate';
import 'angular-aria';
import 'angular-material';
import 'angular-messages';
import 'angular-resource';
import 'swimlane/angular-data-table';

import AppJobsController from 'src/AppJobsController';
import Jobs from 'src/jobs/Jobs';

export default angular.module( 'app', [ 'ngMaterial', 'ngResource', 'ngMessages', Jobs.name ] )
  .config(($mdIconProvider, $mdThemingProvider) => {
    // Register the `avatar` icons
    $mdIconProvider
      .defaultIconSet("./assets/svg/avatars.svg", 128)
      .icon("add", "./assets/svg/round-add-button.svg", 12)
      .icon("close", "./assets/svg/close-button.svg", 12)
      .icon("delete", "./assets/svg/round-delete-button.svg", 12)
      .icon("down", "./assets/svg/down-arrow.svg", 12)
      .icon("ellipsis", "./assets/svg/ellipsis.svg.svg", 12)
      .icon("menu", "./assets/svg/menu.svg", 24)
      .icon("pause", "./assets/svg/rounded-pause-button.svg", 12)
      .icon("restart", "./assets/svg/refresh-button-1.svg", 12)
      .icon("play", "./assets/svg/play-button-inside-a-circle.svg", 12)
      .icon("save", "./assets/svg/save-button.svg", 12)
      .icon("stop", "./assets/svg/stop-button.svg", 12)
      .icon("search", "./assets/svg/searching-magnifying-glass.svg", 12)
      .icon("settings", "./assets/svg/settings-cogwheel-button.svg", 12)
      .icon("up", "./assets/svg/up-arrow.svg", 12);

//    $mdThemingProvider.theme('default')
//      .primaryPalette('brown')
//      .accentPalette('red');
  })
  .controller('AppController', AppJobsController)
  ;
