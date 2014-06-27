'use strict';

angular.module('registryApp')
    .controller('AppCtrl', ['$scope', '$routeParams', '$window', 'Model', function ($scope, $routeParams, $window, Model) {

        $scope.view = {};
        $scope.view.loading = true;
        $scope.view.app = null;

        Model.getApp($routeParams.id).then(function(result) {
            $scope.view.loading = false;
            $scope.view.app = result;
            console.log(result);
        });

        $scope.goBack = function () {
            $window.history.back();
        };


    }]);
