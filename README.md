NOAA
====

[![Build Status](https://travis-ci.org/cloudfoundry/noaa.svg?branch=master)](https://travis-ci.org/cloudfoundry/noaa)


NOAA is a client library to consume metric and log messages from Doppler.


Development
-----------------

Use `go get -d -v -t ./... && ginkgo --race --randomizeAllSpecs --failOnPending --skipMeasurements --cover` to
run the tests.
