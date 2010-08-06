require "fileutils"
require "uri"
require "yaml"

class Puller
  
  require 'open-uri'
  
  FORCE_FETCH = false
  
  U = DataCatalog::ImporterFramework::Utility

  def initialize(handler)
    @handler = handler
    @cache_source_directory = File.join(File.dirname(__FILE__), "/../cache/raw/source/")
    @cache_org_directory    = File.join(File.dirname(__FILE__), "/../cache/raw/org/")
    FileUtils.mkdir_p @cache_source_directory
    FileUtils.mkdir_p @cache_org_directory
    
     @common = {
      :catalog_name => "colorado.ckan.net",
      :catalog_url  => "http://colorado.ckan.net",
    }
  end
  
  def run
    source_data = grab_source_data
    
    sources = build_sources(source_data)
    orgs = build_orgs(source_data)
    
    process_organizations(orgs)
    process_sources(sources)
  end
  
  def grab_source_data
    source_data = []

    grab_source_index_data.each do |source|
      raw_source = grab_one_source(source)
      source_data << raw_source
    end
    
    source_data
  end

  def grab_source_index_data
    url = "http://colorado.ckan.net/api/rest/package"
    fetched_data = open(url).read
    YAML::load(fetched_data)
  end
  
  def grab_one_source(source_name)
    fetched_data = U.parse_json_from_file_or_uri("http://colorado.ckan.net/api/rest/package/" + source_name, @cache_source_directory + source_name, :force_fetch => FORCE_FETCH)
    yaml = YAML::load(fetched_data)
  end


  private

  def build_sources(data)
    sources = []
    data.each do |raw_source|
      source_metadata = {
      :title        => raw_source["title"],
      :description  => raw_source["notes"],
      :url          => raw_source["url"],
      :license      => raw_source["license"],
      :downloads    => standardize_source_downloads(raw_source["resources"]),
      :custom       => build_custom_tags(raw_source["tags"]),
      :frequency    => raw_source["extras"]["update_frequency"],
      #:organization => { :name => raw_source["Agency"] },
      :organization => { :name => get_name(raw_source) },
      :source_type  => standardize_source_type(raw_source["resources"][0]["format"]),
      # :documentation_url => "",
      # :license_url       => "",
      # :released          => "",
      # :period_start      => "",
      # :period_end        => "",
      }

      source_metadata.merge!(@common)    
      sources << source_metadata
    end

    sources
  end
  
  def build_orgs(data)
    orgs = []
    
    data.each do |raw_source|
      org_hash = {
        :name              => get_name(raw_source),
        #:names             => [],
        :acronym           => "",
        :url               => get_url(raw_source),
        :organization      => { :name => "Colorado" },
        :description       => "",
        :org_type          => "governmental",
      }
      org_hash.merge!(@common)
      orgs << org_hash
    end

    orgs.uniq
  end

  def process_organizations(orgs)
    orgs.each do |org|
      @handler.organization(org)
    end
  end

  def process_sources(sources)
    sources.each do |source|
      @handler.source(source)
    end
  end

  def standardize_source_downloads(downloads = [])
    arr = []
    downloads.each do |download|
      format = case download["format"]
        when "REST" then "api"
        when "APP" then "interactive"
      end

      download.delete("description")
      arr << download
    end
    
    arr
  end
  
  def standardize_source_type(source_format)
    source_type = case source_format
      when "APP" then "interactive"
      else "dataset"
    end
  end
  
  def build_custom_tags(tags = [])
    custom = {}
    key = 0
    
    tags.each do |tag|
      custom.merge!(key.to_s => {
        :label       => tag,
        :description => "tag",
        :type        => "tag",
        :value       => tag,
        }
      )
       key += 1
    end
    
    custom
  end
  
  def get_name(data)
    name = data["extras"]["agency"]
    
    if name.nil?
      name = data["maintainer"]
    end
    
    if name == "All"
      name = "Colorado"
    elsif name == "RTD"
      name = "Regional Transportation District (RTD)"
    end
    
    name.strip
  end
  
  def get_names(data)
    names = []
    
    names << data["extras"]["agency"]
    names << data["maintainer"]
    
    names.compact.uniq
  end
  
  def get_url(data)
    if data[:name] == "U.S. Census Bureau"
      return ''
    end
    
    'http://' + URI.parse(data["url"]).host
  end

end
