'use strict';

angular.module('registryApp')
    .controller('ReposCtrl', ['$scope', '$window', 'Repo', 'Header', function ($scope, $window, Repo, Header) {

        Header.setActive('repos');

        /**
         * Callback when repos are loaded
         *
         * @param result
         */
        var reposLoaded = function(result) {

            $scope.view.paginator.prev = $scope.view.page > 1;
            $scope.view.paginator.next = ($scope.view.page * $scope.view.perPage) < result.total;
            $scope.view.total = Math.ceil(result.total / $scope.view.perPage);

            $scope.view.repos = result.items;
            $scope.view.loading = false;

        };

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.repos = [];
        $scope.view.searchTerm = '';

        $scope.view.paginator = {
            prev: false,
            next: false
        };

        $scope.view.page = 1;
        $scope.view.perPage = 25;
        $scope.view.total = 0;

        Repo.getRepos(0).then(reposLoaded);

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

                Repo.getRepos(offset).then(reposLoaded);

            }
        };

        /**
         * Search repos by the term
         */
        $scope.searchRepos = function() {

            $scope.view.page = 1;
            Repo.getRepos(0, $scope.view.searchTerm).then(reposLoaded);

        };

        /**
         * Reset the search
         */
        $scope.resetSearch = function() {

            $scope.view.page = 1;
            $scope.view.searchTerm = '';
            Repo.getRepos(0).then(reposLoaded);

        };


    }]);
