'use strict';

angular.module('registryApp')
    .controller('RepoInstructionsCtrl', ['$scope', '$routeParams', 'Repo', 'Header', function ($scope, $routeParams, Repo, Header) {

        $scope.$parent.view.classes.push('repo-instructions');

        Header.setActive('repos');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.repo = null;

        var repoId = $routeParams.id.replace(/&/g, '/');

        Repo.getRepo(repoId).then(function (repo) {

            $scope.view.repo = Repo.parseUser(repo);
            $scope.view.loading = false;

        });


    }]);
