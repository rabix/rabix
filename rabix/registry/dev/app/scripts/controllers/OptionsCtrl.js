'use strict';

angular.module('registryApp')
    .controller('OptionsCtrl', ['$scope', 'Model', '$modalInstance', 'data', function ($scope, Model, $modalInstance, data) {

        $scope.view = {};
        $scope.view.data = data;
        $scope.view.generating = false;
        $scope.view.revoking = false;
        $scope.view.trace = '';

        $scope.ok = function () {
            $modalInstance.close();
        };

        $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
        };

        /**
         * Generate the token for the user
         */
        $scope.generateToken = function() {
            $scope.view.generating = true;
            Model.generateToken().then(function(result) {

                $scope.view.generating = false;
                $scope.view.trace = 'Your new token: ' + result.token;

            });
        };

        /**
         * Revoke the token of the user
         */
        $scope.revokeToken = function() {
            $scope.view.revoking = true;
            Model.revokeToken().then(function() {

                $scope.view.revoking = false;
                $scope.view.trace = 'Your token has been revoked';


            });
        };

    }]);
