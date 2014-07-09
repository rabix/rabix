'use strict';

angular.module('registryApp')
    .controller('RepoCtrl', ['$scope', '$routeParams', '$window', '$q', 'Repo', 'App', 'Build', 'Header', function ($scope, $routeParams, $window, $q, Repo, App, Build, Header) {

        Header.setActive('repos');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.active = 'apps';
        $scope.view.repo = null;
        $scope.view.apps = [];
        $scope.view.builds = [];


        var repoId = $routeParams.id.replace(/&/g, '/');

        $q.all([
            Repo.getRepo(repoId),
            App.getApps(),
            Build.getBuilds()
        ]).then(function (result) {

            $scope.view.loading = false;

            $scope.view.repo = result[0];
            $scope.view.apps = result[1].items;
            $scope.view.builds = result[2].items;
        });

        /**
         * Go back to the previous screen
         */
        $scope.goBack = function () {
            $window.history.back();
        };

        $scope.switchTab = function (active) {
            $scope.view.active = active;
        };

    }]);
