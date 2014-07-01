'use strict';

angular.module('registryApp')
    .controller('BuildCtrl', ['$scope', '$routeParams', '$window', '$interval', 'Build', 'Header', function ($scope, $routeParams, $window, $interval, Build, Header) {

        var logIntervalId;

        Header.setActive('builds');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.build = null;
        $scope.view.tab = angular.isUndefined($routeParams.tab) ? 'details': $routeParams.tab;

        Build.getBuild($routeParams.id).then(function(result) {
            $scope.view.loading = false;
            $scope.view.build = result;
        });

        /**
         * Go back to the previous screen
         */
        $scope.goBack = function () {
            $window.history.back();
        };

        $scope.view.log = [];
        _.times(100, function(i) {
            $scope.view.log.push('Line ' + i);
        });

        var logLoaded = function(result) {

            console.log('logLoaded ', $scope.view.skip, status);

            //if (result.status !== 'running') {
            if ($scope.view.skip / 25 > 10) {
                $scope.stopLogInterval();
            }

            $scope.view.log = $scope.view.log.concat(result);
            $scope.view.skip += 25;

        };

        if ($routeParams.tab === 'log') {

            $scope.view.log = [];
            $scope.view.skip = 0;


            logIntervalId = $interval(function() {
                Build.getLog($scope.view.skip).then(logLoaded);
            }, 1000);
        }

        $scope.stopLogInterval = function() {
            if (angular.isDefined(logIntervalId)) {
                $interval.cancel(logIntervalId);
                logIntervalId = undefined;
                console.log('logIntervalId canceled');
            }
        };

        $scope.$on('$destroy', function() {
            $scope.stopLogInterval();
        });


    }]);
