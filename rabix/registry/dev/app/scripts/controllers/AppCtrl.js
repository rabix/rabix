'use strict';

angular.module('registryApp')
    .controller('AppCtrl', ['$scope', '$routeParams', '$window', 'App', 'Header', function ($scope, $routeParams, $window, App, Header) {

        Header.setActive('apps');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.app = null;

        App.getApp($routeParams.id).then(function(result) {
            $scope.view.loading = false;
            $scope.view.app = result;
        });

        $scope.goBack = function () {
            $window.history.back();
        };


    }]);
