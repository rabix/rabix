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
                templateUrl: 'views/home.html',
                controller: 'HomeCtrl'
            })
            .when('/apps', {
                templateUrl: 'views/apps.html',
                controller: 'AppsCtrl'
            })
            .when('/apps/:repo', {
                templateUrl: 'views/apps.html',
                controller: 'AppsCtrl'
            })
            .when('/app/:id', {
                templateUrl: 'views/app.html',
                controller: 'AppCtrl'
            })
            .when('/builds', {
                templateUrl: 'views/builds.html',
                controller: 'BuildsCtrl'
            })
            .when('/build/:id', {
                templateUrl: 'views/build.html',
                controller: 'BuildCtrl'
            })
            .when('/repos', {
                templateUrl: 'views/repos.html',
                controller: 'ReposCtrl'
            })
            .when('/repo/:id', {
                templateUrl: 'views/repo.html',
                controller: 'RepoCtrl'
            })
            .when('/settings', {
                templateUrl: 'views/settings.html',
                controller: 'SettingsCtrl'
            })
            .otherwise({
                redirectTo: '/'
            });

        $httpProvider.interceptors.push('HTTPInterceptor');
    }]);
