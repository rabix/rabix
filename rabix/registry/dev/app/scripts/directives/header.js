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
                console.log('header');

            }
        };
    }]);