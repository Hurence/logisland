
export default {
    name: 'topicList',
    config: {
        bindings: { topics: '<', mode: '<', selected: '<', showDetails: '&onSelected' },
        templateUrl: 'src/topic-list/topic-list.template.html',
        controller: ['TopicsDataService', '$log', '$scope', '$timeout', '$mdDialog', TopicListController]
    }
};

function TopicListController(TopicsDataService, $log, $scope, $timeout, $mdDialog) {
    var vm = $scope;
    var self = this;

    self.selectedTopic = null;
    self.topics = refreshTopics();
    self.filteredTopics = self.topics;

    self.simulateQuery = false;
    self.isDisabled = false;

    self.querySearch = querySearch;
    self.selectedItemChange = selectedItemChange;
    self.searchTextChange = searchTextChange;
    self.searchText = '';
    self.pressEnter = pressEnter;
    self.refreshTopics = refreshTopics;


    function refreshTopics() {

        return TopicsDataService.query(function(result){
           if(result.length > 0){
               $log.debug(result);
           }
        });


    }


    // ******************************
    // Internal methods
    // ******************************

    /**
     * Search for repos... use $timeout to simulate
     * remote dataservice call.
     */
    function querySearch(query) {
        self.searchText = query;
        self.filteredTopics = query
            ? self.topics.filter(createFilterFor(query))
            : self.topics;

        return self.filteredTopics;
    }

    /**
     * Create filter function for a query string
     */
    function createFilterFor(query) {
        var lowercaseQuery = angular.lowercase(query);

        return function filterFn(item) {
            return (angular.lowercase(item.name).search(lowercaseQuery) != -1) ||
                (angular.lowercase(item.documentation).search(lowercaseQuery) != -1);
        };

    }

    function pressEnter() {
        var autoChild = document.getElementById('Auto').firstElementChild;
        var el = angular.element(autoChild).blur();
        querySearch(self.searchText);
    }
    function searchTextChange(text) {
       querySearch(text);
    }

    function selectedItemChange(item) {
        if (item != undefined) {
            $log.info('Item changed to ' + JSON.stringify(item));
            querySearch(item.name);
        }

    }







    vm.formatDate = formatDate;
    vm.deleteTopic = deleteTopic;

    vm.serializers = [
        { id: 'none', name: 'none' },
        { id: 'com.hurence.logisland.serializer.AvroSerializer', name: 'avro' },
        { id: 'com.hurence.logisland.serializer.KryoSerializer', name: 'kryo' }
    ];
    vm.selectedSerializer = { id: 'com.hurence.logisland.serializer.KryoSerializer', name: 'kryo' };

    vm.formatDate = formatDate;
    vm.saveTopic = saveTopic;
    vm.convertToJson = convertToJson;
    vm.convertToString = convertToString;

    $log.debug("Topic selected");

    function formatDate(instant) {
        return new Date(instant);
    }

    function saveTopic(topic) {
        $log.debug("Saving topic");
        TopicsDataService.save(topic);
        self.topics = refreshTopics();
    }

    function convertToJson(val) {
        val = JSON.parse(val);
        return val
    }

    function convertToString(val) {
        return JSON.stringify(val);
    }



    function deleteTopic(ev, topic) {

        // Appending dialog to document.body to cover sidenav in docs app
        var confirm = $mdDialog.confirm()
            .title('Would you like to delete topic ' + topic.name + '?')
            .textContent('The topic will be definitively removed from Kafka with all its messages. \n' +
            'If this topic is being processed by any stream a crash might occur.')
            .ariaLabel('confirm delete')
            .targetEvent(ev)
            .ok('Delete')
            .cancel('cancel');

        $mdDialog.show(confirm).then(function () {
            $log.debug("Deleting topic " + topic.name);
            TopicsDataService.delete({ id: topic.name }, function () {
                $log.debug("deleted");
                self.searchText = '';
                self.topics = refreshTopics();
            });
        }, function () {
            $scope.status = 'You decided to keep your debt.';
        });



        //      self.topics.splice(index, 1);
    }



}