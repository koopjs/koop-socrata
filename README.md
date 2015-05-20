## Socrata Provider for [Koop](https://github.com/Esri/koop)

This provider makes it possible to access [Socrata's JSON API](http://dev.socrata.com/docs/formats/json.html) as either GeoJSON or an Esri FeatureService. This is particular useful for making maps and doing analysis on the web.

## Install

To install/use this provider you first need a working installation of [Koop](https://github.com/Esri/koop). Then from within the koop directory you'll need to run the following:

```
npm install https://github.com/koopjs/koop-socrata/tarball/master
```

## Add your app key
1. Go to dev.socrata.com/register to create an app key
2. Edit the default.json in your koop-app config to add
```json
{
	"socrata": {
		token: 'your-app-token'
	}
}
```
## Register Socrata Hosts

Once this provider's been installed you need to "register" a particular instance of Socrata with your Koop instance. To do this you make `POST` request to the `/socrata` endpoint like so: 

```
curl --data "host=https://data.nola.gov&id=nola" localhost:1337/socrata
```
*for Windows users, download cURL from http://curl.haxx.se/download.html or use a tool of your choice to generate the POST request*

What you'll need for that request to work is an ID and the URL of the Socrata instance. The ID is what you'll use to reference datasets that come from Socrata in Koop.

To make sure this works you can visit: http://localhost:1337/socrata and you should see all of the register hosts. 

## Access Socrata Data

To access a dataset hosted in Socrata you'll need a "Resource ID" from Socrata. Datasets in Socrata can be accessed as raw JSON like this:

* [https://data.nola.gov/Health-Education-and-Social-Services/NOLA-Grocery-Stores/fwm6-d78i](https://data.nola.gov/Health-Education-and-Social-Services/NOLA-Grocery-Stores/fwm6-d78i) translates into -> https://data.nola.gov/resource/fwm6-d78i.json

And then the ID `fwm6-d78i` can be referenced in Koop like so:

http://koop.dc.esri.com/socrata/nola/fwm6-d78i

If your Socrata data has more than one location column, you can specify the desired location column in the http request like this:

https://path_to_koop/socrata/socrataProvider/dataSetID!spatialColumn

## Handle Large Datasets

The Socrata API defaults to 1000 results per request, but can be set to return up to 50,000. Koop will page through large datasets to capture all the points. To change the number of results per request, modify the 'limit' variable in the socrata.getResoruce function in models/Socrata.js.

## Examples 

Here are a few examples of data hosted in Socrata and accessed via Koop.

* GeoJSON: http://koop.dc.esri.com/socrata/nola/fwm6-d78i
* FeatureService: http://koop.dc.esri.com/socrata/nola/fwm6-d78i/FeatureServer/0
* All publicly registered Socrata instances: http://koop.dc.esri.com/socrata
