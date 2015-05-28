# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.2.1] - 2015-05-28
### Changed 
* Respecting resultOffset, resultOffsetCount, and limit/offset when querying featureservices 

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

## [0.1.0] - 2015-04-21
### Changed
* This project now uses `standard` as its code formatting
* Keeping a legit changelog
* Added tape testing with sinon stubs in the controller tests

[0.2.1]: https://github.com/Esri/koop/releases/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Esri/koop/releases/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/Esri/koop/releases/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Esri/koop/releases/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Esri/koop/releases/tag/v0.1.0
