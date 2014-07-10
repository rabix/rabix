'use strict';

angular.module('registryApp')
    .controller('SettingsCtrl', ['$scope', '$timeout', 'Header', 'User', function ($scope, $timeout, Header, User) {

        var generateTimeoutId;
        var revokeTimeoutId;

        Header.setActive('settings');

        $scope.view = {};
        $scope.view.generating = false;
        $scope.view.revoking = false;
        $scope.view.trace = {generate: '', revoke: ''};
        $scope.view.token = '';

        /**
         * Generate the token for the user
         */
        $scope.generateToken = function() {
            $scope.view.generating = true;
            User.generateToken().then(function(result) {

                $scope.view.generating = false;
                $scope.view.trace.generate = 'You successfully generated new token';
                $scope.view.token = result.token;

                $scope.cancelGenerateTimeout();
                generateTimeoutId = $timeout(function () {
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

                $scope.view.revoking = false;
                $scope.view.trace.revoke = 'Your token has been revoked';
                $scope.view.token = '';

                $scope.cancelRevokeTimeout();
                revokeTimeoutId = $timeout(function () {
                    $scope.view.trace.revoke = '';
                }, 3000);

            });
        };

        User.getToken().then(function (result) {
            $scope.view.token = result.token;
        });

        /**
         * Cancel token generate timeout
         */
        $scope.cancelGenerateTimeout = function () {
            if (angular.isDefined(generateTimeoutId)) {
                $timeout.cancel(generateTimeoutId);
                generateTimeoutId = undefined;
            }
        };

        /**
         * Cancel token revoke timeout
         */
        $scope.cancelRevokeTimeout = function () {
            if (angular.isDefined(revokeTimeoutId)) {
                $timeout.cancel(revokeTimeoutId);
                revokeTimeoutId = undefined;
            }
        };

        $scope.$on('$destroy', function () {
            $scope.cancelGenerateTimeout();
            $scope.cancelRevokeTimeout();
        });



    }]);
