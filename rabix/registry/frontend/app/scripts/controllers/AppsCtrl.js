'use strict';

angular.module('registryApp')
    .controller('AppsCtrl', ['$scope', '$routeParams', 'App', 'Header', function ($scope, $routeParams, App, Header) {

        Header.setActive('apps');

        /**
         * Callback when apps are loaded
         *
         * @param result
         */
        var appsLoaded = function(result) {

            $scope.view.paginator.prev = $scope.view.page > 1;
            $scope.view.paginator.next = ($scope.view.page * $scope.view.perPage) <= result.total;
            $scope.view.total = Math.ceil(result.total / $scope.view.perPage);

            $scope.view.apps = result.items;
            $scope.view.loading = false;
        };

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.apps = [];
        $scope.view.searchTerm = '';
        if ($routeParams.repo) {
            $scope.view.repo = $routeParams.repo.replace(/&/g, '/');
        }

        $scope.view.paginator = {
            prev: false,
            next: false
        };

        $scope.view.page = 1;
        $scope.view.perPage = 25;
        $scope.view.total = 0;

        App.getApps(0, '', $routeParams.repo).then(appsLoaded);

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

                App.getApps(offset, $scope.view.searchTerm, $routeParams.repo).then(appsLoaded);

            }
        };

        /**
         * Search the apps by the term
         */
        $scope.searchApps = function() {

            $scope.view.page = 1;
            App.getApps(0, $scope.view.searchTerm, $routeParams.repo).then(appsLoaded);

        };

        /**
         * Reset the search
         */
        $scope.resetSearch = function() {

            $scope.view.page = 1;
            $scope.view.searchTerm = '';
            App.getApps(0, '', $routeParams.repo).then(appsLoaded);

        };

    }]);
