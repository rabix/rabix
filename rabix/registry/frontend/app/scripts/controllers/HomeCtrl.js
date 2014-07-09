'use strict';

angular.module('registryApp')
    .controller('HomeCtrl', ['$scope', 'Header', 'User', function ($scope, Header, User) {

        Header.setActive('home');

        $scope.view = {};
        $scope.view.loading = true;

        User.getUser().then(function(result) {
            $scope.view.user = User.parseUser(result);
            $scope.view.loading = false;
        });

    }]);
