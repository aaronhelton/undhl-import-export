#!/bin/env ruby
# encoding: UTF-8

# This script should be run with ruby version > 1.9.3 and the following libraries and gems

require 'rubygems'
require 'aws-sdk'
require 'trollop'
require 'json'
require 'digest/md5'

##### Begin Argument Parsing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
s3_json_to_ddb.rb scans the supplied bucket and folder path for .json files and creates DynamoDB entries for each one.  If a .error file is present, it notes this as well and adds a missing field containing JSON formatted details for the missing files.

This script requires both S3 credentials and DynamoDB credentials.  

This script will not create entries that already exist in the database.

Typical usage:

  s3_json_to_ddb.rb --s3-credentials <file> --ddb-credentials <file> --s3-bucket <bucket> --s3-prefix <prefix>

Usage: 
  s3_json_to_ddb.rb [options]

where options are:
EOS

  opt :aws_credentials, "Path to a credentials file for AWS that can read and write to both S3 and DynamoDB", :type => String
  opt :s3_bucket, "Name of the S3 Bucket you wish to use", :type => String
  opt :s3_prefix, "Path to the folder you wish to scan", :type => String
end
Trollop::die :aws_credentials, "<file> must be supplied" unless opts[:aws_credentials]
Trollop::die :aws_credentials, "<file> must exist" unless File.exist?(opts[:aws_credentials]) && opts[:aws_credentials]
Trollop::die :s3_bucket, "<bucket> is a required argument" unless opts[:s3_bucket]
Trollop::die :s3_prefix, "<prefix> is a required argument" unless opts[:s3_prefix]

if !File.exists?('tmp')
  Dir.mkdir('tmp') or abort "Unable to create tmp directory.  Check your permissions and try again."
end

AWSCREDS = JSON.parse(File.read(opts[:aws_credentials]))
ddb = AWS::DynamoDB.new(
  :access_key_id => AWSCREDS["accessKeyId"],
  :secret_access_key => AWSCREDS["secretAccessKey"]
)
db_table = ddb.tables['Documents']
db_table.hash_key = [:id, :string]
s3 = AWS::S3.new(
  :access_key_id => AWSCREDS["accessKeyId"],
  :secret_access_key => AWSCREDS["secretAccessKey"]
)
s3_bucket = opts[:s3_bucket]
s3_prefix = opts[:s3_prefix]
bucket = s3.buckets[s3_bucket]
tree = bucket.objects.with_prefix(s3_prefix).as_tree
directories = tree.children.select(&:branch?).collect(&:prefix)

directories.each do |dir|
  symbols_tree = bucket.objects.with_prefix(dir).as_tree
  symbols = symbols_tree.children.select(&:branch?).collect(&:prefix)
  symbols.each do |sym|
    package_dir = sym
    bucket.objects.with_prefix(sym).each do |s|
      metadata = Hash.new
      package_status = "Complete"
      if s.key =~ /\.json/
        metadata = JSON.parse(s.read)
        error_metadata = nil
        error_fname = s.key.gsub(/\.json/,'.error')
        #if bucket.objects.with_prefix(sym)[error_fname].exists?
        #  error_metadata = JSON.parse(bucket.objects.with_prefix(sym)[error_fname].read)
        #  package_status = "Incomplete"
        #end
        docsymbol = nil
        if metadata["symbols"] && metadata["symbols"].first =~ /JOURNAL/ && metadata["symbols"].size > 1
          docsymbol = metadata["symbols"].join(" ")
        else
          docsymbol = metadata["symbols"].first
        end
        metadata["languages"].each do |lang|
          item_id = Digest::MD5.hexdigest([docsymbol, lang].join(' '))
          item_hash = Hash.new
          item_hash['id'] = item_id
          item_hash['Document Symbol'] = docsymbol
          item_hash['Language'] = lang
          item_hash['Issued Date'] = metadata["issued_date"]
          if metadata["title"].size > 0
            item_hash['Title'] = metadata["title"]
          end
          if metadata["agen_num"].size > 0 then
            item_hash['Agenda'] = metadata["agen_num"]
          end
          if metadata["distribution"].size > 0
            item_hash['Distribution'] = metadata["distribution"]
          end
          if metadata["doc_num"].size > 0
            item_hash['Doc Num'] = metadata["doc_num"]
          end
          if metadata[lang]["job_num"].size > 0
            item_hash['Job Num'] = metadata[lang]["job_num"]
          end
          if metadata["isbn"].size > 0
            item_hash['ISBN'] = metadata["isbn"]
          end
          if metadata["issn"].size > 0
            item_hash['ISSN'] = metadata["issn"]
          end
          if metadata["cr_sales_num"].size > 0
            item_hash['CR Sales Num'] = metadata["cr_sales_num"]
          end
          if metadata["slot_num"].size > 0
            item_hash['Slot Num'] = metadata["slot_num"]
          end
          item_hash['S3 File Path'] = "#{package_dir}#{metadata[lang]["filename"].to_s}"
          file_status = "Exists"
          if bucket.objects["#{item_hash['S3 File Path']}"].exists?
            file_status = "Exists"
            item_hash["S3 File Status"] = file_status
            package_status = "Complete"
          else
            file_status = "Missing file #{item_hash['S3 File Path']}"
            item_hash["S3 File Status"] = file_status
            package_status = "Incomplete"
          end 
          if metadata["missing_english"] == "true"
            package_status = "Incomplete"
          end
          item_hash["Package Status"] = package_status
          item = db_table.items.create( item_hash )
          puts "#{item_id}: #{docsymbol} #{lang} - #{file_status}"
        end
        puts "\t#{package_dir}: #{package_status}"
        sleep(3)
      end
    end
  end
end
