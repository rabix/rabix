'use strict';

angular.module('registryApp')
    .controller('SettingsCtrl', ['$scope', '$window', 'Header', 'User', function ($scope, $window, Header, User) {

        Header.setActive('settings');

        $scope.view = {};
        $scope.view.generating = false;
        $scope.view.revoking = false;
        $scope.view.trace = '';

        /**
         * Go back to the previous screen
         */
        $scope.goBack = function () {
            $window.history.back();
        };

        /**
         * Generate the token for the user
         */
        $scope.generateToken = function() {
            $scope.view.generating = true;
            User.generateToken().then(function(result) {

                $scope.view.generating = false;
                $scope.view.trace = 'Your new token: <strong>' + result.token + '</strong>';

            });
        };

        /**
         * Revoke the token of the user
         */
        $scope.revokeToken = function() {
            $scope.view.revoking = true;
            User.revokeToken().then(function() {

                $scope.view.revoking = false;
                $scope.view.trace = 'Your token has been revoked';

            });
        };



    }]);
