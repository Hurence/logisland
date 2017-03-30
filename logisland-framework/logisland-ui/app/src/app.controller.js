/**
 * App Controller for the LogIsland UI
 */

export default ['JobDataService', 'TopicsDataService', 'ListService', 'AppSettings', '$mdSidenav', '$mdToast', '$log', '$scope', '$state', AppController];

function AppController(JobDataService, TopicsDataService, ListService, AppSettings, $mdSidenav, $mdToast, $log, $scope, $state, $topicsList) {
    var self = this;
    var vm = $scope;

    self.version = AppSettings.version;
    self.appPath = 'TITLE';

    self.selectedProcessor = null;


    self.isFabOpen = false;

    self.menuItems = [{ name: "Start", direction: "right", icon: "play" },
    { name: "Stop", direction: "right", icon: "stop" }];


    self.toggleList = ListService.toggle;
    self.showSimpleToast = showSimpleToast;


    function showSimpleToast(message) {

        $mdToast.show(
            $mdToast.simple()
                .textContent(message)
                .position('top right')
                .hideDelay(3000)
        );
    }


}