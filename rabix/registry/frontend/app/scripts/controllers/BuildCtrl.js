'use strict';

angular.module('registryApp')
    .controller('BuildCtrl', ['$scope', '$routeParams', '$window', '$interval', 'Build', 'Header', function ($scope, $routeParams, $window, $interval, Build, Header) {

        var logIntervalId;

        Header.setActive('builds');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.build = null;
        $scope.view.tab = angular.isUndefined($routeParams.tab) ? 'details': $routeParams.tab;

        /* get the build details */
        Build.getBuild($routeParams.id).then(function(result) {

            $scope.view.build = result;

            /* and if in log tab */
            if ($routeParams.tab === 'log') {

                $scope.view.log = [];
                $scope.view.contentLength = 0;

                // TODO remove this later
                // mock begin
//                var isSuccess = _.random(0, 1);
//                if (isSuccess) {
//                    result.status = 'running';
//                }
                // mock end

                /* start log polling if build is running */
                if (result.status === 'running') {

                    $scope.view.loading = false;

                    logIntervalId = $interval(function() {

                        console.log('log polling started');
                        Build.getLog($routeParams.id, $scope.view.contentLength).then(logLoaded);

                    }, 1000);

                } else {
                    /* other than that take the log for the current build */
                    Build.getLog($routeParams.id, 0).then(function(result) {
                        $scope.view.loading = false;
                        $scope.view.log.push(result.content);
                    });
                }
            } else {
                $scope.view.loading = false;
            }
        });

        /**
         * Go back to the previous screen
         */
        $scope.goBack = function () {
            $window.history.back();
        };

        /**
         * Callback when log for the build is loaded
         *
         * @param result
         */
        var logLoaded = function(result) {

            // TODO remove this later
            // mock begin
//            var isSuccess = _.random(0, 1);
//            if (isSuccess) {
//                result.status = 'running';
//            }
            // mock end

            if (result.status !== 'running') {
                $scope.stopLogInterval();
            }

            $scope.view.build.status = result.status;

            console.log('log polling at ', $scope.view.contentLength);

            // TODO remove this later
            // mock begin
//            result.content = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.\n Nam ut augue nec elit dignissim tristique. Maecenas ipsum velit, egestas a elit a, feugiat euismod diam.\n Vestibulum eu felis vel odio faucibus euismod. Curabitur tincidunt volutpat sagittis.\n Phasellus suscipit facilisis accumsan. Phasellus sed purus ac nunc condimentum gravida.\n Nunc blandit sit amet tellus sed malesuada.\n Proin dapibus orci vitae purus laoreet, at rutrum magna blandit.';
            // mock end

            $scope.view.log = $scope.view.log.concat(result.content.split('\n'));
            $scope.view.contentLength = result.contentLength;

        };

        /**
         * Stop the log polling
         */
        $scope.stopLogInterval = function() {
            if (angular.isDefined(logIntervalId)) {
                $interval.cancel(logIntervalId);
                logIntervalId = undefined;
                console.log('log polling canceled');
            }
        };

        $scope.$on('$destroy', function() {
            $scope.stopLogInterval();
        });


    }]);
