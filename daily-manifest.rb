#!/bin/env ruby
# encoding: UTF-8

require 'pdf-reader'
require 'trollop'

opts = Trollop::options do
       banner <<-EOS
daily-manifest.rb outputs a daily list of documents published since <date> or from <date> to <date>.

Usage:
  daily-manifest.rb [options]

where options are:
EOS

  opt :aws_credentials, "(Required) Path to a JSON-formatted credentials file that can read and write to both S3 and DynamoDB.", :type => String
  opt :from_date, "(Required) Date to begin processing.", :type => String
  opt :to_date, "Date to end processing.", :type => String
end
Trollop::die :aws_credentials, "<file> must be supplied" unless opts[:aws_credentials]
Trollop::die :aws_credentials, "<file> must exist.  Check your path and try again" unless File.exists?(opts[:aws_credentials])
Trollop::die :from_date, "<date> is required" unless opts[:from_date]
Trollop::die :from_date, "<date> must be in the format YYYY-MM-DD, e.g., 2013-11-01" unless opts[:from_date] =~ /(\d+)-(\d+)-(\d+)/
Trollop::die :to_date, "<date> must be in the format YYYY-MM-DD, e.g., 2013-11-01" unless opts[:to_date] =~ /(\d+)-(\d+)-(\d+)/ if opts[:to_date]

AWSCREDS = JSON.parse(File.read(opts[:aws_credentials]))
AWS.config( access_key_id: AWSCREDS["accessKeyId"], secret_access_key: AWSCREDS["secretAccessKey"] )


