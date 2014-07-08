'use strict';

angular.module('registryApp')
    .controller('BuildCtrl', ['$scope', '$routeParams', '$window', '$interval', '$document', '$timeout', 'Build', 'Header', function ($scope, $routeParams, $window, $interval, $document, $timeout, Build, Header) {

        var logIntervalId;
        var scrollTimeoutId;

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

                    console.log('log polling started');

                    $scope.view.loading = false;

                    logIntervalId = $interval(function() {
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

            if (result.contentLength > 0) {
                $scope.view.log = $scope.view.log.concat(result.content.split('\n'));
                $scope.view.contentLength += parseInt(result.contentLength, 10);

                $scope.stopScrollTimeout();

                var logContainer = $document[0].getElementById('log-content');
                scrollTimeoutId = $timeout(function () {
                    logContainer.scrollTop = logContainer.scrollHeight;
                }, 100);
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


        /**
         * Stop the scroll timeout
         */
        $scope.stopScrollTimeout = function() {
            if (angular.isDefined(scrollTimeoutId)) {
                $timeout.cancel(scrollTimeoutId);
                scrollTimeoutId = undefined;
            }
        };

        $scope.$on('$destroy', function() {
            $scope.stopLogInterval();
            $scope.stopScrollTimeout();
        });


    }]);
