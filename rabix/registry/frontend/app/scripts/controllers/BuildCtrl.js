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

                /* start log polling if build is running */
                if (result.status === 'running') {

                    $scope.view.loading = false;

                    logIntervalId = $interval(function() {

                        console.log('log polling started');
                        Build.getLog($routeParams.id, $scope.view.contentLength).then(logLoaded);

                    }, 2000);

                } else {
                    /* other than that take the log for the current build */
                    Build.getLog($routeParams.id, 0).then(function(result) {
                        $scope.view.loading = false;
                        $scope.view.log = $scope.view.log.concat(result.content.split('\n'));
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

            if (result.status !== 'running') {
                $scope.stopLogInterval();
            }

            $scope.view.build.status = result.status;

            console.log('log polling at ', $scope.view.contentLength);

            $scope.view.log = $scope.view.log.concat(result.content.split('\n'));
            if (result.contentLength > 0) {
                $scope.view.contentLength = result.contentLength;
            }

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
