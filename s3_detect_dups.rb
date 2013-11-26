#!/usr/bin/ruby
# encoding: UTF-8

# This script should be run with ruby version > 1.9.3 and the following libraries and gems

require 'rubygems'
require 'smarter_csv'	#External ruby gem.  gem install smarter_csv to install it
require 'net/http'
require 'aws/s3'	#External ruby gem.  gem install aws-s3 to install it
require 'date'
require 'pdf-reader'	#External ruby gem.  gem install pdf-reader to install it
require 'trollop'	#External ruby gem.  gem install trollop to install it
require 'rexml/document'
require 'json'

##### Begin Argument Parsing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
s3_detect_dups.rb looks across all of the known metadata/bitstream packages in the given bucket and detects any duplicate document symbols.  It doesn't make any modifications!

Note: The credentials file should be in the format bucket::access_key_id::secret_access_key.  Note the separator '::'

Usage: 
	s3_detect_dups.rb [options]

where options are: 
EOS

	opt :s3_credentials, "Path to a credentials file for S3.", :type => String
end
Trollop::die :s3_credentials, "<file> must be supplied" unless opts[:s3_credentials] 
Trollop::die :s3_credentials, "<file> must exist" unless File.exist?(opts[:s3_credentials]) && opts[:s3_credentials]

# This file has been verified to exist.  It doesn't mean it contains actual credentials, though.
credentials = File.read(opts[:s3_credentials]).split('::')

bucket = credentials[0]
access_key_id = credentials[1]
secret_access_key = credentials[2]
AWS::S3::Base.establish_connection!(
	:access_key_id => access_key_id,
	:secret_access_key => secret_access_key
)

packages_prefix = "Drop/packages"
docsymbols = Array.new
dsmap = Hash.new
AWS::S3::Bucket.find(bucket).objects( :prefix => packages_prefix ).each do |file|
	if file.key =~ /\.json/
		docsymbols << file.key.split(/\//).last.gsub(/\.json/,'')
		dsmap[file.key] = { "value" => file.key.split(/\//).last.gsub(/\.json/,'') }
	end
end

#files.each do |file|
#	if file.key =~ /\.json/
#		#p file.key.split(/\//).last.gsub(/\.json/,'')
#		docsymbols << file.key.split(/\//).last.gsub(/\.json/,'')
#		dsmap[file.key] = { "value" => file.key.split(/\//).last.gsub(/\.json/,'') }
#	end
#end

dups = docsymbols.group_by {|e| e}.select { |k,v| v.size > 1}.keys
if dups.size > 0
	dups.each do |dup|
		dsmap.keys.each do |k|
			if k =~ /#{dup}/
				p k
			end
		end
	end
else
	p "No duplicate docsymbols detected."
end