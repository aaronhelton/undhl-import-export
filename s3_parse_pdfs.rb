#!/usr/bin/ruby
# encoding: UTF-8

# This script should be run with ruby version > 1.9.3 and the following libraries and gems
#irb(main):094:0> files.each do |file|
#irb(main):095:1* if file != "." && file != ".."
#irb(main):096:2> reader = PDF::Reader.new("testfiles/#{file}")
#irb(main):097:2> puts "#{file}\t#{reader.info[:Symbol1]}\t#{reader.info[:CreationDate]}"
#irb(main):098:2> end
#irb(main):099:1> end

#{:CreationDate=>"D:20131122132552-05'00'", :Symbol1=>"DP/2014/6", :Author=>"RTPU User", :Creator=>"Acrobat PDFMaker 7.0.7 for Word", :Producer=>"Acrobat Distiller 7.0.5 (Windows)", :ModDate=>"D:20131122132554-05'00'", :Operator=>"Nikitina", :Company=>"United Nations", :JobNo=>"1357116", :Title=>" ", :DraftPages=>"4 "}


require 'rubygems'
require 'net/http'
require 'aws/s3'	#External ruby gem.  gem install aws-s3 to install it
require 'date'
require 'pdf-reader'	#External ruby gem.  gem install pdf-reader to install it
require 'trollop'	#External ruby gem.  gem install trollop to install it
require 'json'

def s3_find_docsymbol

end

def extract_metadata(file)
	metadata = Hash.new
	docsymbol = 'NULL'
	language = "Other/Undetermined"
	reader = PDF::Reader.new(file)
	if reader.info.has_key?(:Symbol1)
		docsymbol = reader.info[:Symbol1]
	end
	##{reader.info[:Operator]}
	auth_title = "#{reader.info[:Author]} #{reader.info[:Title]}".downcase
	if auth_title =~ /atpu/
		language = "Arabic"
	elsif auth_title =~ /ctpu/
		language = "Chinese"
	elsif auth_title =~ /etpu/
		language = "English"
	elsif auth_title =~ /ftpu/
		language = "French"
	elsif auth_title =~ /rtpu/
		language = "Russian"
	elsif auth_title =~ /stpu/
		language = "Spanish"
	end
	cd = reader.info[:CreationDate].gsub(/D\:/,'')
	date_created = "#{cd[0..3]}-#{cd[4..5]}-#{cd[6..7]}"
	metadata = {	:symbol => docsymbol,
						:language => language,
						:date_created => date_created }
	return metadata
end

##### Begin Argument Parsing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
s3_parse_pdsf.rb Polls S3 for unpackaged PDFs and moves them to another folder, where they are arranged by document symbol.  For good measure, the script also extracts what metadata it can from the PDF.  Documents with no symbols cannot be processed automatically, but most documents have symbols.

Note: The credentials file should be in the format bucket::access_key_id::secret_access_key.  Note the separator '::'

Usage: 
	s3_parse_pdfs.rb [options]

where options are: 
EOS

	opt :s3_credentials, "Path to a credentials file for S3.", :type => String
	#opt :since_month, "Numerical month to begin calculations.  Default is current month.", :type => Integer
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

prefix = "Drop/N"
tempdir = "testfiles"
metadata_set = Array.new

#Download the files we find with the above prefix
puts "Found #{AWS::S3::Bucket.objects(bucket, :prefix => prefix).size} files.  Downloading to #{tempdir}."
AWS::S3::Bucket.objects(bucket, :prefix => prefix).each do |file|
	outfile = "#{file.key.gsub(/Drop\//,"#{tempdir}/")}"
	File.open(outfile, 'w') do |f|
		AWS::S3::S3Object.stream(file.key, bucket) do |chunk|
			f.write chunk
		end
	end
	#get the metadata
	metadata_set << extract_metadata(outfile)
	#and delete the temp file
	File.delete(outfile)
end

metadata_set.each do |m|
	if m.symbol == "NULL"
		# no symbol here, move along
	else
		#check our existing folders to see if it exists somewhere
		
	end
end