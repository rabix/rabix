'use strict';

angular.module('registryApp')
    .directive('height', ['$window', '$timeout', function ($window, $timeout) {
        return {
            link: function(scope, element) {

                /**
                 * Set the height of the content container
                 */
                var setHeight = function() {

                    var height = element[0].parentNode.offsetHeight - (scope.header + scope.footer);

                    element[0].style.height = height + 'px';
                };

                var timeoutId = $timeout(function() {

                    scope.header = element[0].parentNode.children[0].offsetHeight;
                    scope.footer = angular.isDefined(element[0].parentNode.children[2]) ? element[0].parentNode.children[2].offsetHeight : 0;

                    setHeight();
                });

                var lazySetHeight = _.debounce(setHeight, 150);

                angular.element($window).on('resize', lazySetHeight);

                element.on('$destroy', function() {
                    angular.element($window).off('resize', lazySetHeight);
                    $timeout.cancel(timeoutId);
                });

            }
        };
    }]);