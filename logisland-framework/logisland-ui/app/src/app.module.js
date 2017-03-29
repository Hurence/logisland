// Load libraries
import angular from 'angular';

import 'angular-animate';
import 'angular-aria';
import 'angular-material';
import 'angular-messages';
import 'angular-resource';
import 'angular-ui-router';
import 'angular-xeditable';

import AppController from 'src/app.controller';
import Jobs from 'src/jobs/Jobs';
import Topics from 'src/topic-list/topic-list.module';
import NavigationToolbar from 'src/toolbars/navigation-toolbar.component';
import HeaderToolbar from 'src/toolbars/header-toolbar.component';

export default angular
    .module('app', ['ngMaterial', 'ngResource', 'ui.router', 'ngMessages', 'xeditable', Jobs.name, Topics.name])
    .component(NavigationToolbar.name, NavigationToolbar.config)
    .component(HeaderToolbar.name, HeaderToolbar.config)
    .config(($mdIconProvider, $mdDateLocaleProvider, $mdThemingProvider, $urlRouterProvider, $stateProvider) => {

        $mdIconProvider
            .icon("add", "./assets/svg/round-add-button.svg", 12)
            .icon("close", "./assets/svg/close-button.svg", 12)
            .icon("delete", "./assets/svg/delete.svg", 24)
            .icon("down", "./assets/svg/down-arrow.svg", 12)
            .icon("ellipsis", "./assets/svg/ellipsis.svg.svg", 12)
            .icon("menu", "./assets/svg/menu.svg", 24)
            .icon("pause", "./assets/svg/rounded-pause-button.svg", 12)
            .icon("restart", "./assets/svg/refresh-button-1.svg", 12)
            .icon("play", "./assets/svg/play-button-inside-a-circle.svg", 12)
            .icon("save", "./assets/svg/content-save.svg", 12)
            .icon("stop", "./assets/svg/stop-button.svg", 12)
            .icon("search", "./assets/svg/searching-magnifying-glass.svg", 12)
            .icon("settings", "./assets/svg/settings-cogwheel-button.svg", 12)
            .icon("menu", "./assets/svg/menu.svg", 24)
            .icon("settings", "./assets/svg/settings.svg", 24)
            .icon("charts", "./assets/svg/chart-areaspline.svg", 512)
            .icon("hangouts", "./assets/svg/hangouts.svg", 512)
            .icon("export", "./assets/svg/export.svg", 20)
            .icon("job", "./assets/svg/job.svg", 20)
            .icon("database", "./assets/svg/database.svg", 24)
            .icon("gauge", "./assets/svg/gauge.svg", 24)
            .icon("alert", "./assets/svg/lightbulb.svg", 24)
            .icon("flash", "./assets/svg/flash.svg", 24);

        $mdThemingProvider.theme('default');
        $mdThemingProvider.theme('docs-dark')
            .primaryPalette('yellow')
            .dark();
        $urlRouterProvider.otherwise('/topics');

        $stateProvider
            .state({
                abstract: true,
                name: 'jobs',
                url: '/jobs',
                template: '<ui-view/>'
            })
            //            .state({
            //                name: 'jobs.list',
            //                url: '/list',
            //                template: '<jobs-list></jobs-list>'
            //            })
            .state({
                name: 'jobs.details',
                url: '/details',
                template: '<job-details flex selected="app.selectedJob"> </job-details>'
            })
            .state({
                name: 'topics',
                url: '/topics',
                template: '<topics-list flex topics="app.topics"></topics-list>'
            });

    })
    .constant('_', window._)
    .controller('AppController', AppController)
    ;
