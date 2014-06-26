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
        'ui.bootstrap'
    ])
    .config(function ($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/main.html',
                controller: 'MainCtrl'
            })
            .when('/repo/:username/:repoName', {
                templateUrl: 'views/main.html',
                controller: 'MainCtrl'
            })
            .when('/app/:id', {
                templateUrl: 'views/app.html',
                controller: 'AppCtrl'
            })
            .otherwise({
                redirectTo: '/'
            });
    });
