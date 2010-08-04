# National Data Catalog Importer for texas.gov

## Installation

### Install Needed RubyGems

    # For development and production:
    gem install datacatalog-importer
    
    # For testing:
    gem install rspec
    
### Set Up Config Files

* Create `config.yml` using `config.example.yml` as a template.

### Run the Tests

* Run `rake test`

## Usage

First, pull. Then, push. It's easy.

### 1. rake pull

Download and parse pages from texas.gov. You will need to specify the environment using the IMPORTER_ENV environment variable. Some examples:

* `IMPORTER_ENV=local rake pull`
* `IMPORTER_ENV=sandbox rake pull`
* `IMPORTER_ENV=production rake pull`

### 2. rake push

Upload pulled data to the National Data Catalog API. You will need to specify the environment using the IMPORTER_ENV environment variable. Some examples:

* `IMPORTER_ENV=local rake push`
* `IMPORTER_ENV=sandbox rake push`
* `IMPORTER_ENV=production rake push`

## Join The National Data Catalog Community

* the [National Data Catalog Web site](http://nationaldatacatalog.com)
* the [National Data Catalog mailing list](http://groups.google.com/group/datacatalog)
* the [National Data Catalog project page](http://sunlightlabs.com/projects/datacatalog/)
* the [Sunlight Labs mailing list](http://groups.google.com/group/sunlightlabs)
* the [transparency chat room](irc://chat.freenode.net/transparency)
