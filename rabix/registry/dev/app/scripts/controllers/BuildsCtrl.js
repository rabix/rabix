'use strict';

angular.module('registryApp')
    .controller('BuildsCtrl', ['$scope', '$routeParams', '$window', 'Build', 'Header', function ($scope, $routeParams, $window, Build, Header) {

        Header.setActive('builds');

        var buildsLoaded = function(result) {

            $scope.view.paginator.prev = $scope.view.page > 1;
            $scope.view.paginator.next = ($scope.view.page * $scope.view.perPage) <= result.total;

            $scope.view.builds = result.items;
            $scope.view.loading = false;
        };

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.builds = [];
        if ($routeParams.repo) {
            $scope.view.repo = $routeParams.repo.replace(/&/g, '/');
        }

        $scope.view.paginator = {
            prev: false,
            next: false
        };

        $scope.view.page = 1;
        $scope.view.perPage = 25;

        Build.getBuilds(0, $routeParams.repo).then(buildsLoaded);



    }]);
