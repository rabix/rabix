'use strict';

angular.module('registryApp')
    .controller('AddYourGitHubRepoCtrl', ['$scope', '$timeout', '$location', '$filter', 'Repo', 'Header', function ($scope, $timeout, $location, $filter, Repo, Header) {

        Header.setActive('repos');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.repos = [];

        Repo.getHitHubRepos().then(function(result) {
            // TODO remove timeout when api handler ready
            $timeout(function () {
                $scope.view.loading = false;
                $scope.view.repos = result.items;
            }, 300);
        });

        /**
         * Add repo to the user
         * @param repo
         */
        $scope.addRepo = function (repo) {

            repo.adding = true;

            Repo.addRepo(repo.id).then(function() {
                repo.adding = true;
                // TODO remove hardcoded url once the api handler is ready
                //$location.path('/repo-instructions/' + $filter('encode')(repo.id));
                $location.path('/repo-instructions/' + $filter('encode')('rabix/bedtools2'));

            });

        };


    }]);
