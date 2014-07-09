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

                /**
                 * Prepare variables for height calculation
                 */
                var setVariables = function () {
                    scope.header = element[0].parentNode.children[0].offsetHeight;
                    scope.footer = angular.isDefined(element[0].parentNode.children[2]) ? element[0].parentNode.children[2].offsetHeight : 0;
                };

                var timeoutId = $timeout(function() {
                    setVariables();
                    setHeight();
                });

                var lazySetHeight = _.debounce(setHeight, 150);

                angular.element($window).on('resize', lazySetHeight);

                element.on('$destroy', function() {

                    angular.element($window).off('resize', lazySetHeight);

                    if (angular.isDefined(timeoutId)) {
                        $timeout.cancel(timeoutId);
                        timeoutId = undefined;
                    }
                });

                scope.$watch('view.tab', function (n, o) {
                    if (n !== o) {
                        setVariables();
                        setHeight();
                    }
                });

            }
        };
    }]);