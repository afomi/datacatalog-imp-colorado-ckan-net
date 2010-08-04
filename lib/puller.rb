class Puller
  
  require 'open-uri'
  
  FORCE_FETCH = false
  
  U = DataCatalog::ImporterFramework::Utility

  def initialize(handler)
    @handler = handler
    @cache_source_directory = File.dirname(__FILE__) + "/../cache/raw/source/"
    @cache_org_directory    = File.dirname(__FILE__) + "/../cache/raw/org/"
    FileUtils.mkdir_p @cache_source_directory
    FileUtils.mkdir_p @cache_org_directory
  end
  
  def run
    @common = {
      :catalog_name => "colorado.ckan.net",
      :catalog_url  => "http://colorado.ckan.net",
    }
    
    prepare_sources_and_orgs
    process_organizations(@orgs)
    process_sources(@sources)
  end
  
  
  # == SOURCES
  
  def prepare_sources_and_orgs
    @source_refs = grab_source_index_data
    
    @sources = []
    @orgs    = []
    
    @source_refs.each do |source|
      raw_source = grab_one_source(source)
      
      #raise raw_source.inspect
            
      source_hash = {
      :title       => raw_source["title"],
      :description => raw_source["notes"],
      :url         => raw_source["url"],
      :license     => raw_source["license"],
      :downloads   => standardize_source_downloads(raw_source["resources"]),
      :custom      => build_source_tags(raw_source["tags"]),
      :frequency   => raw_source["extras"]["update_frequency"],
      # :source_type => "",
      # :documentation_url => "",
      # :license_url       => "",
      # :released          => "",
      # :period_start      => "",
      # :period_end        => "",
      }
      
      source_hash.merge!(@common)    
      @sources << source_hash
      
      org_hash = {
        :name              => raw_source["extras"]["agency"],
        :acronym           => "",
        :url               => 'http:' + URI.parse(raw_source["url"]).host,
        :organization      => { :name => "Colorado" },
        :description       => "",
        :org_type          => "not-for-profit",
      }
      org_hash = org_hash.merge(@common)
      @orgs << org_hash
    end
    
  end

  def grab_source_index_data
    url          = "http://colorado.ckan.net/api/rest/package"
    fetched_data = open(url).read
    yaml         = YAML::load(fetched_data)
    puts "Found " + yaml.length.to_s + " source(s)"
    return yaml
  end
  
  def grab_one_source(source_name)
    fetched_data = U.parse_json_from_file_or_uri("http://colorado.ckan.net/api/rest/package/" + source_name, @cache_source_directory + source_name, :force_fetch => FORCE_FETCH)
    yaml = YAML::load(fetched_data)
  end


  private


  def process_organizations(orgs)
    orgs.uniq!.each do |org|
      puts org.inspect
      puts org[:name]
      unless org[:name] == "All" or org[:name] == nil
        @handler.organization(org)
      end
    end
  end
  
  def process_sources(sources)
    sources.each do |source|
      @handler.source(source)
    end
  end
  
  # Args:
  # Array of hashes containing downloads
  # Returns: 
  # Array of hashes with valid "formats", without "description"
  def standardize_source_downloads(downloads = [])
    
    arr = []
    downloads.each do |download|
      if download["format"] == "REST"
        download["format"] = "api"
      elsif download["format"] == "APP"
        download["format"] = "interactive"
      end
      
      download.delete("description")
      arr << download
    end
    
    return arr
  end
  
  def build_source_tags(tags = [])
    arr  = []
    tags.each do |tag|
      arr << {
        :label       => tag,
        :description => "tag",
        :type        => "tag",
        :value       => tag,
       }
    end
    
    return arr
  end

end
