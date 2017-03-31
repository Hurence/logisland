
export default {
    name: 'headerToolbar',
    config: {
        bindings: { selected: '<' },
        templateUrl: 'src/toolbars/header-toolbar.template.html',
        controller: ['AppSettings', 'JobDataService', 'TopicsDataService', '$log', '$scope', '$state', '$mdDialog', HeaderToolbarController]
    }
};

function HeaderToolbarController(AppSettings, JobDataService, TopicsDataService, $log, $scope, $state, $mdDialog) {
    var vm = $scope;
    var self = this;

    self.addTopic = addTopic;
    self.addJob = addJob;
    self.topics = [];

   


    function addTopic(ev) {


        // Appending dialog to document.body to cover sidenav in docs app
        var confirm = $mdDialog.prompt()
            .title("Let's create a brand new topic")
            .textContent('Give a meaningful name')
            .placeholder('Topic name')
            .ariaLabel('topic name')
            .initialValue('new_topic')
            .targetEvent(ev)
            .ok('Create')
            .cancel('Cancel');

        $mdDialog.show(confirm).then(function (result) {

            var newTopic = AppSettings.DEFAULT_TOPIC;
            newTopic.name = result;

            TopicsDataService.save(newTopic, function () {
                $state.reload();
            });
        }, function () {
            $log.debug("cancel topic creation")
        });
    }

    

    function addJob(ev) {



        TopicsDataService.query(function (result) {

            self.topics = result.map(function (element) {
                return element.name;
            });

              // Appending dialog to document.body to cover sidenav in docs app
          var confirm = $mdDialog.prompt()
              .title("Let's create a brand new job")
              .textContent('Give a meaningful name')
              .placeholder('Job name')
              .ariaLabel('Job name')
              .initialValue('new_job')
              .targetEvent(ev)
              .ok('Create')
              .cancel('Cancel');
  
          $mdDialog.show(confirm).then(function (result) {
  
              var newJob = AppSettings.DEFAULT_JOB;
              newJob.name = result;
              newJob.streams[0] = AppSettings.DEFAULT_PIPELINE;
              JobDataService.save(newJob, function () {
                  $state.reload();
              });
          }, function () {
              $log.debug("cancel job creation")
          });

        });
    }
        

        /*   $mdDialog.show({
                controller: DialogController,
                templateUrl: 'src/toolbars/create-job.template.html',
                parent: angular.element(document.body),
                targetEvent: ev,
                clickOutsideToClose: true,
                fullscreen: true // Only for -xs, -sm breakpoints.
            })
                .then(function (answer) {
                    $scope.status = 'You said the information was "' + answer + '".';
                }, function () {
                    $scope.status = 'You cancelled the dialog.';
                });

    function DialogController($scope, $mdDialog) {


                $scope.newJobName = 'new_job';
                        $scope.topics =  TopicsDataService.query().map(function (element) {
                            return element.name;
                        });
                

                    $scope.hide = function () {
                        console.log("hide");
                        $mdDialog.hide();
                    };

                    $scope.cancel = function () {
                        $mdDialog.cancel();
                    };

                    $scope.answer = function (answer) {
                        $mdDialog.hide(answer);
                    };
                }
        });*/

   
};
