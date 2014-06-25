'use strict';

angular.module('registryApp')
    .directive('header', ['$templateCache', function ($templateCache) {
        return {
            restrict: 'E',
            replace: true,
            template: $templateCache.get('views/partials/header.html'),
            scope: {},
            link: function (scope) {

                scope.view = {};
                //scope.view.user = {};
                scope.view.user = {username: 'test'};

                scope.logIn = function() {

                };


            }
        };
    }]);