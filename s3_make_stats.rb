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
s3_make_stats.rb creates a statistical report of packages, complete and incomplete, that are available in S3.

Note: The credentials file should be in the format bucket::access_key_id::secret_access_key.  Note the separator '::'

Usage: 
	s3_make_stats.rb [options]

where options are: 
EOS

	opt :s3_credentials, "Path to a credentials file for S3.", :type => String
	opt :since_month, "Numerical month to begin calculations.  Default is current month.", :type => Integer
	# should have daily/today, monthy/this month, yearly/this year, etc?
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
this_year = Date.today.year
this_month = Date.today.month
this_day = Date.today.day
if opts[:since_month]
	p opts[:since_month]
else
	#Default path prefix is Drop/#{this_year}-#{this_month}
	i = 1
	daily_packages = Hash.new
	while i <= this_day
		#Get all of this month's actual prefixes.  This is bounded by the number of possible days in a month.
		if i < 10 
			idx = "0#{i.to_s}"
		else
			idx = i
		end
		this_symbol = Hash.new
		prefix = "#{packages_prefix}/#{this_year}-#{this_month}-#{idx}/"
		AWS::S3::Bucket.objects(bucket, :prefix => prefix).each do |file|
			if file.key =~ /\.json/
				symbol = file.key.split(/\//).last.gsub(/\.json/,'')
				this_symbol["#{symbol}"] = { 
					"date" => "#{this_year}-#{this_month}-#{idx}",
					"metadata_file" => file.key,
					"symbol" => symbol
				}
			end
		end
		if this_symbol.size > 0
			daily_packages["#{this_year}-#{this_month}-#{idx}"] = this_symbol
		end
		#daily_packages({"#{this_year}-#{this_month}-#{idx}"}) << this_daily
		i = i + 1
	end
	
	daily_packages.keys.each do |k|
		incomplete = 0
		puts "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
		daily_packages[k].keys.each do |pk|
			path = "Drop/packages/#{k}/#{pk}"
			symbol = pk
			error_file = "#{path}/#{symbol}.error"
			if AWS::S3::S3Object.exists?(error_file,bucket)
				incomplete = incomplete + 1
				status = "Incomplete"
			else
				status = "Complete"
			end
		end
		puts "On #{k}, there was/were #{daily_packages[k].size} document(s) issued."
		puts "Of these, #{incomplete} are missing at least one file."
		puts "Symbol(s) issued:\n\t #{daily_packages[k].keys.join(",\n\t")}"		
	end
end