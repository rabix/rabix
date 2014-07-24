'use strict';

angular.module('registryApp')
    .controller('AppCtrl', ['$scope', '$routeParams', 'App', 'Header', function ($scope, $routeParams, App, Header) {

        $scope.$parent.view.classes.push('app');

        Header.setActive('apps');

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.app = null;

        App.getApp($routeParams.id).then(function(result) {
            $scope.view.loading = false;
            $scope.view.app = result;
        });


    }]);
