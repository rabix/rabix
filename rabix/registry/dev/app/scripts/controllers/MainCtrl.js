'use strict';

/**
 * @ngdoc function
 * @name registryApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the registryApp
 */
angular.module('registryApp')
    .controller('MainCtrl', ['$scope', '$routeParams', 'Model', function ($scope, $routeParams, Model) {


        /**
         * Callback when apps are loaded
         *
         * @param result
         */
        var appsLoaded = function(result) {

            // TODO replace this value with the total value from the api request
            var total = 126;

            $scope.view.paginator.prev = $scope.view.page > 1;
            $scope.view.paginator.next = (($scope.view.page - 1) * $scope.view.perPage) < total;

            $scope.view.apps = result.items;
            $scope.view.loading = false;
        };

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.apps = [];
        $scope.view.searchTerm = '';

        $scope.view.paginator = {
            prev: false,
            next: false
        };

        $scope.view.page = 1;
        $scope.view.perPage = 25;

        Model.getApps(0, '', $routeParams.username, $routeParams.repoName).then(appsLoaded);

        /**
         * Go to the next/prev page
         *
         * @param dir
         */
        $scope.goToPage = function(dir) {

            if (!$scope.view.loading) {

                if (dir === 'prev') {
                    $scope.view.page -= 1;
                }
                if (dir === 'next') {
                    $scope.view.page += 1;
                }

                $scope.view.loading = true;
                var offset = ($scope.view.page - 1) * $scope.view.perPage;

                Model.getApps(offset, $scope.view.searchTerm, $routeParams.username, $routeParams.repoName).then(appsLoaded);

            }
        };

        /**
         * Search the apps by the term
         */
        $scope.searchApps = function() {

            Model.getApps(0, $scope.view.searchTerm, $routeParams.username, $routeParams.repoName).then(appsLoaded);

        };

        /**
         * Reset the search
         */
        $scope.resetSearch = function() {

            $scope.view.searchTerm = '';
            Model.getApps(0, '', $routeParams.username, $routeParams.repoName).then(appsLoaded);

        };

    }]);
