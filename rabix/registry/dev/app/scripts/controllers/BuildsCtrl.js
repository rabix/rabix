'use strict';

angular.module('registryApp')
    .controller('BuildsCtrl', ['$scope', '$routeParams', '$window', 'Build', 'Header', function ($scope, $routeParams, $window, Build, Header) {

        Header.setActive('builds');

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


    }]);
