'use strict';

/**
 * @ngdoc overview
 * @name registryApp
 * @description
 * # registryApp
 *
 * Main module of the application.
 */
angular
    .module('registryApp', [
        'ngAnimate',
        'ngCookies',
        'ngResource',
        'ngRoute',
        'ngSanitize',
        'ui.bootstrap',
        'ngPrettyJson'
    ])
    .config(['$routeProvider', '$httpProvider', function ($routeProvider, $httpProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/apps.html',
                controller: 'AppsCtrl'
            })
            .when('/repo/:repo', {
                templateUrl: 'views/main.html',
                controller: 'MainCtrl'
            })
            .when('/app/:id', {
                templateUrl: 'views/app.html',
                controller: 'AppCtrl'
            })
            .when('/builds', {
                templateUrl: 'views/builds.html',
                controller: 'BuildsCtrl'
            })
            .otherwise({
                redirectTo: '/'
            });

        $httpProvider.interceptors.push('HTTPInterceptor');
    }]);
