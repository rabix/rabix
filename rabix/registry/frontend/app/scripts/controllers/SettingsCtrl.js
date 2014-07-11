'use strict';

angular.module('registryApp')
    .controller('SettingsCtrl', ['$scope', '$timeout', 'Header', 'User', function ($scope, $timeout, Header, User) {

        var tokenTimeoutId;

        Header.setActive('settings');

        $scope.view = {};
        $scope.view.generating = false;
        $scope.view.revoking = false;
        $scope.view.getting = false;
        $scope.view.trace = {generate: '', revoke: '', token: ''};

        /**
         * Generate the token for the user
         */
        $scope.generateToken = function() {
            $scope.view.generating = true;
            User.generateToken().then(function() {

                $scope.cancelTokenTimeout();

                $scope.view.generating = false;
                $scope.view.trace.generate = 'You successfully generated new token';

                tokenTimeoutId = $timeout(function () {
                    $scope.view.trace.generate = '';
                }, 3000);

            });
        };

        /**
         * Revoke the token of the user
         */
        $scope.revokeToken = function() {
            $scope.view.revoking = true;
            User.revokeToken().then(function() {

                $scope.cancelTokenTimeout();

                $scope.view.revoking = false;
                $scope.view.trace.revoke = 'Your token has been revoked';

                tokenTimeoutId = $timeout(function () {
                    $scope.view.trace.revoke = '';
                }, 3000);

            });
        };

        /**
         * Get the current token for the user
         */
        $scope.getToken = function () {
            $scope.view.getting = true;
            User.getToken().then(function(result) {

                $scope.cancelTokenTimeout();

                $scope.view.getting = false;
                $scope.view.trace.token = result.token;

                tokenTimeoutId = $timeout(function () {
                    $scope.view.trace.token = '';
                }, 3000);

            });
        };

        /**
         * Cancel token timeout
         */
        $scope.cancelTokenTimeout = function () {
            if (angular.isDefined(tokenTimeoutId)) {
                $scope.view.trace = {generate: '', revoke: '', token: ''};
                $timeout.cancel(tokenTimeoutId);
                tokenTimeoutId = undefined;
            }
        };

        $scope.$on('$destroy', function () {
            $scope.cancelTokenTimeout();
        });

    }]);
