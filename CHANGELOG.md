# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## 1.0.2 - 2015-12-29
### Changed
* bumped version of leaflet/esri leaflet used by preview
* cleaned up views/demo.ejs
* ensured log files created by passing tests have valid filenames and are ignored by git

### Fixed
* Fields are no longer erroneously cast to date

## [1.0.1] - 2015-07-29
### Fixed
* Cache expiration really works now

## [1.0.0] - 2015-07-28
### Fixed
* Alternate location fields are respected
* Cache now expires correctly

## [0.4.1] - 2015-07-02
### Fixed
* Feature service count respects JSONP

## [0.4.0] - 2015-06-10
### Added
* Non-point spatial datasets are now supported

### Changed
* Refactored processing logic

## [0.3.4] - 2015-06-09
### Fixed
* Bug with failed stats requests to use a fallback request to the resource

## [0.3.3] - 2015-06-09
### Fixed
* No exception thrown when requesting a service that is processing
* Can get resources that are filtered views of other resources
* Not to send repsonse after processing has been sent

## [0.3.2] - 2015-06-09
### Fixed
* Do not check for count on brand new services

## [0.3.1] - 2015-06-08
### Added
* 502 response contains error messages
* New unit and integration tests + fixtures
* Logging all errors and progress

### Changed
* Feature service counts occur without fetching features from the cache
* Report 202 immediately when processing new dataset
* JSON is processed in streams
* All requests are made with gzip
* Requests are made for table metadata, count and first row before paging
* Default page limit is set to 10,000 can be controlled in Koop Config

### Fixed
* Cache expiration is checked every 24 hours
* Duplicate data is no longer entered when making quick sequential requests for a new dataset

## [0.2.2] - 2015-05-28
### Changed
* Respecting resultOffset, resultOffsetCount, and limit/offset when querying featureservices

## [0.2.1] - 2015-05-08
### Added
* Allows provider to make requests using an app token, if available

## [0.2.0] - 2015-05-08
### Changed
* Implements a new paging style that uses an async request queue to control how many requests we make at once
* Adds the concept of returning 202 response much like koop-agol when paging is in progress

## [0.1.2] - 2015-05-07
### Changed
* Changed the order in which object properties get flattened. Location objects were getting flattened too early in the process and cause location lookups to fail when building the geojson

## [0.1.1] - 2015-05-06
### Added
* Flattening all object based properties
* ensuring that each feature contains each field

## 0.1.0 - 2015-04-21
### Changed
* This project now uses `standard` as its code formatting
* Keeping a legit changelog
* Added tape testing with sinon stubs in the controller tests

[1.0.2]: https://github.com/koopjs/koop-socrata/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/koopjs/koop-socrata/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/koopjs/koop-socrata/compare/v0.4.1...v1.0.0
[0.4.1]: https://github.com/koopjs/koop-socrata/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/koopjs/koop-socrata/compare/v0.3.3...v0.4.0
[0.3.4]: https://github.com/koopjs/koop-socrata/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/koopjs/koop-socrata/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/koopjs/koop-socrata/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/koopjs/koop-socrata/compare/v0.2.2...v0.3.1
[0.2.2]: https://github.com/koopjs/koop-socrata/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/koopjs/koop-socrata/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/koopjs/koop-socrata/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/koopjs/koop-socrata/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/koopjs/koop-socrata/compare/v0.1.0...v0.1.1
