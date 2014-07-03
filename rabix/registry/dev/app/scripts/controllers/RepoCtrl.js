'use strict';

angular.module('registryApp')
    .controller('RepoCtrl', ['$scope', '$routeParams', '$window', 'Repo', 'Header', function ($scope, $routeParams, $window, Repo, Header) {

        Header.setActive('repos');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.repo = null;

        var repoId = $routeParams.id.replace(/&/g, '/');

        Repo.getRepo(repoId).then(function(result) {
            $scope.view.loading = false;
            $scope.view.repo = result;
        });

        /**
         * Go back to the previous screen
         */
        $scope.goBack = function () {
            $window.history.back();
        };


    }]);
