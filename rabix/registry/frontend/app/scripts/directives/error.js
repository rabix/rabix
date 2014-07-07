'use strict';

angular.module('registryApp')
    .directive('error', ['$templateCache', function ($templateCache) {
        return {
            restrict: 'E',
            replace: true,
            template: $templateCache.get('views/partials/error.html'),
            link: function(scope) {

                scope.errorMessages = [];

                scope.$on('httpError', function (obj, message) {
                    if (scope.errorMessages.indexOf(message) === -1) {
                        scope.errorMessages.push(message);
                    }
                });

                /**
                 * Close the error alert
                 */
                scope.closeError = function () {
                    scope.errorMessages = [];
                };

            }
        };
    }]);