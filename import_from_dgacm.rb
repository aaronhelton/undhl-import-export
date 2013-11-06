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

# Start with some utility functions
def log(message)
	logfile = "logs/import.log"
	File.open(logfile, 'a+') do |f|
		f.puts "#{Time.now} -- #{message}"
	end
end

def get_agenda(agenda_key)
	log("Looking up agenda text for #{agenda_key}")
	agenda_text = nil
	xmldoc = REXML::Document.new File.read 'lib/agenda.xml'
	a = xmldoc.elements["//agendas/agenda/item[@id='#{agenda_key}']/label"].text
	if a
		log(a)
		return a
	else
		return nil
	end
end

def lxcode(language)
	case language
		when "Arabic"
			iso = "AR"
		when "Chinese"
			iso = "ZH"
		when "English"
			iso = "EN"
		when "French"
			iso = "FR"
		when "Russian"
			iso = "RU"
		when "Spanish"
			iso = "ES"
	end
	return iso
end

def fetch_latest_csv(access_key_id, secret_access_key)
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)

	files = AWS::S3::Bucket.find('undhl-dgacm').objects( :prefix => 'Drop' )

	epoch_time = DateTime.parse('1970-01-01T00:00:00+00:00')
	latest_time = epoch_time
	latest_file = ''

	files.each do |file|
		if file.key.downcase =~ /^drop\/dhl/
			mtime = DateTime.parse("#{file.about["last-modified"]}")
			if mtime > latest_time
				latest_file = file.key
			end
		end
	end
	log("Getting latest CSV file, found #{latest_file.gsub(/Drop\//,'')}")
	File.open(latest_file.gsub(/Drop\//,''), 'w') do |f|
		AWS::S3::S3Object.stream(latest_file, 'undhl-dgacm') do |chunk|
			f.write chunk
		end
	end
	AWS::S3::S3Object.rename(latest_file, latest_file.gsub(/Drop\//, 'Drop/processed/'), 'undhl-dgacm')
	return latest_file.gsub(/Drop\//,'')
end

def fetch_specific_file(access_key_id, secret_access_key, bucket, filename,dir)
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)

	#files = AWS::S3::Bucket.find('undhl-dgacm').objects( :prefix => 'Drop' )
	if AWS::S3::S3Object.exists?("#{filename}",'undhl-dgacm')
		log("Found #{filename} in S3 bucket.")
		outfile = filename.split('/').last
		File.open(dir + "/" + outfile, 'w') do |f|
			AWS::S3::S3Object.stream(filename, 'undhl-dgacm') do |chunk|
				f.write chunk
			end
		end
	else
		Trollop::die "Remote file doesn't exist.  Check the supplied filename and try again."
	end
	return outfile
end

def remote_exists?(access_key_id, secret_access_key, bucket, filename)
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	if AWS::S3::S3Object.exists?("#{filename}",'undhl-dgacm')
		return true
	else
		return false
	end
end

def write_to_s3(access_key_id, secret_access_key, bucket, filename, data)
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	AWS::S3::S3Object.store(filename, data, bucket)
end

def s3_move(access_key_id, secret_access_key, bucket, filename, destination)
	AWS::S3::Base.establish_connection!(
		:access_key_id => access_key_id,
		:secret_access_key => secret_access_key
	)
	AWS::S3::S3Object.rename(filename, destination, bucket)
end

def parse_csv(csv_file)
	debug = 0		#REALLY avoid this unless you need it.  Keep it at 0!!!  You've been warned.
	
	transitional = Hash.new{ |h,k| h[k] = Hash.new(&h.default_proc) }
	metadata = Hash.new
	csv_data = SmarterCSV.process(csv_file, { :col_sep => "\t", :file_encoding => 'utf-8', :verbose => true })
	csv_data.each do |row|
		if row[:symbol] =~ /\s\s/
			docsymbol = row[:symbol].split(/\s+/)[0]
		else
			docsymbol = row[:symbol]
		end
		case row[:lang]
			when "A"
				language = "Arabic"
			when "C"
				language = "Chinese"
			when "E"
				language = "English"
			when "F"
				language = "French"
			when "R"
				language = "Russian"
			when "S"
				language = "Spanish"
			else
				language = "Other"
		end
		transitional[docsymbol][language] = row
	end
	metadata = Hash.new{ |h,k| h[k] = Hash.new(&h.default_proc) }
	docsymbols = transitional.keys
	docsymbols.each do |ds|
		agenda = ''
		languages = transitional[ds].keys
		if transitional[ds].key?("English") 
			if transitional[ds]["English"][:distribution].to_s.strip.downcase == "GENERAL".downcase
				if 	transitional[ds]["English"][:agen_num]
					agenda = transitional[ds]["English"][:agen_num].to_s
				end
				metadata[ds] = {
				"doc_num" => transitional[ds]["English"][:doc_num].to_s.strip,
				"title" => transitional[ds]["English"][:title].to_s.strip,
				"symbols" => transitional[ds]["English"][:symbol].to_s.strip.split(/\s+/),
				"distribution" => transitional[ds]["English"][:distribution].to_s.strip,
				"isbn" => transitional[ds]["English"][:isbn].to_s.strip,
				"issn" => transitional[ds]["English"][:issn].to_s.strip,
				"cr_sales_num" => transitional[ds]["English"][:cr_sales_num].to_s.strip,
				"issued_date" => transitional[ds]["English"][:publication_date].to_s,
				"slot_num" => transitional[ds]["English"][:slot_num].to_s.strip,
				"agen_num" => agenda, 
				"languages" =>  languages }
			
				#Get the non-unique stuff next
				languages.each do |language|
					jn = transitional[ds][language][:job_num].strip.gsub(/NY-J-/,'N').gsub(/-/,'').gsub(/LP/,'').gsub(/\*/,'').gsub(/\/1/,'').gsub(/X/,'')
					metadata[ds][language] = { 
						"job_num" => transitional[ds][language][:job_num].to_s.strip,
						"filename" => jn + ".pdf",
						"language" => language
					}
				end
			end
		end
	end
	if debug == 1
		metadata.each do |m|
			log(m) 
		end
	end
	return metadata
end

def write_metadata(file, dir, data)
	if !File.exists?(dir)
		Dir.mkdir(dir) or abort "Unable to create #{dir}.  Make sure you have permissions to write to it and try again."
	end
	File.open("#{dir}/#{file}", "w+") do |f|
		f.puts(data)
	end
end

##### Begin Argument Parsing and Procedural Logic #####
opts = Trollop::options do
	banner <<-EOS
import_from_dgacm.rb processes DGCAM-originated metadata files (CSV format) and creates DSpace Simple Archive Packages from them.

Note: The credentials file should be in the format bucket::access_key_id::secret_access_key.  Note the separator '::'

Usage: 
	import_from_dgacm.rb [options]

where options are: 
EOS

	opt :remote, "Get the file from the remote S3 bucket."
	opt :get_latest, "Instead of specifying a filename, tell the script to get the latest available metadata file."
	opt :local, "Get the metadata file from a local file system."
	opt :credentials, "Path to a credentials file for S3.", :type => String
	opt :filename, "Filename containing metadata to parse.  Required if specifying a local file.", :type => String
end
Trollop::die :credentials, "<file> must be supplied" unless opts[:credentials] 
Trollop::die :credentials, "<file> must exist" unless File.exist?(opts[:credentials]) && opts[:credentials]
Trollop::die :filename, "is required" if opts[:local] && !opts[:filename]
Trollop::die :filename, "must exist" unless File.exist?(opts[:filename]) if opts[:local] && opts[:filename]

if !File.exists?('logs')
	Dir.mkdir('logs') or abort "Unable to create logs directory.  Make sure you have permission to write to it and try again."
end

if opts[:credentials] && File.exist?(opts[:credentials])
	CREDS = File.read(opts[:credentials]).split('::')
end

log("Beginning DGACM packaging")
#Standard option 1: remote with credentials, get latest slotted file
if opts[:remote] && opts[:get_latest] 
	file = fetch_latest_csv(CREDS[1],CREDS[2])
end

#Standard option 2: remote with credentials, get specific file (doesn't matter slotted vs non-slotted)
if opts[:remote] && opts[:filename]
	file = fetch_specific_file(CREDS[1],CREDS[2],CREDS[0],opts[:filename],"./")
end

#Standard option 3: local specific file (best for processing errors)
if opts[:local] && opts[:filename]
	file = opts[:filename]
end

ENTITY_REPLACE = '<!ENTITY quot   "&#34;"><!ENTITY nbsp   "&#160;"><!ENTITY iexcl  "&#161;"><!ENTITY cent   "&#162;"><!ENTITY pound  "&#163;"><!ENTITY curren "&#164;"><!ENTITY yen   "&#165;"><!ENTITY brvbar "&#166;"><!ENTITY sect   "&#167;"><!ENTITY uml    "&#168;"><!ENTITY copy   "&#169;"><!ENTITY ordf   "&#170;"><!ENTITY laquo  "&#171;"><!ENTITY not    "&#172;"><!ENTITY shy    "&#173;"><!ENTITY reg    "&#174;"><!ENTITY macr   "&#175;"><!ENTITY deg    "&#176;"><!ENTITY plusmn "&#177;"><!ENTITY sup2   "&#178;"><!ENTITY sup3   "&#179;"><!ENTITY acute  "&#180;"><!ENTITY micro  "&#181;"><!ENTITY para   "&#182;"><!ENTITY middot "&#183;"><!ENTITY cedil  "&#184;"><!ENTITY sup1   "&#185;"><!ENTITY ordm   "&#186;"><!ENTITY raquo  "&#187;"><!ENTITY frac14 "&#188;"><!ENTITY frac12 "&#189;"><!ENTITY frac34 "&#190;"><!ENTITY iquest "&#191;"><!ENTITY Agrave "&#192;"><!ENTITY Aacute "&#193;"><!ENTITY Acirc  "&#194;"><!ENTITY Atilde "&#195;"><!ENTITY Auml   "&#196;"><!ENTITY Aring  "&#197;"><!ENTITY AElig  "&#198;"><!ENTITY Ccedil "&#199;"><!ENTITY Egrave "&#200;"><!ENTITY Eacute "&#201;"><!ENTITY Ecirc  "&#202;"><!ENTITY Euml   "&#203;"><!ENTITY Igrave "&#204;"><!ENTITY Iacute "&#205;"><!ENTITY Icirc  "&#206;"><!ENTITY Iuml  "&#207;"><!ENTITY ETH    "&#208;"><!ENTITY Ntilde "&#209;"><!ENTITY Ograve "&#210;"><!ENTITY Oacute "&#211;"><!ENTITY Ocirc  "&#212;"><!ENTITY Otilde "&#213;"><!ENTITY Ouml  "&#214;"><!ENTITY times  "&#215;"><!ENTITY Oslash "&#216;"><!ENTITY Ugrave "&#217;"><!ENTITY Uacute "&#218;"><!ENTITY Ucirc  "&#219;"><!ENTITY Uuml   "&#220;"><!ENTITY Yacute "&#221;"><!ENTITY THORN  "&#222;"><!ENTITY szlig  "&#223;"><!ENTITY agrave "&#224;"><!ENTITY aacute "&#225;"><!ENTITY acirc  "&#226;"><!ENTITY atilde "&#227;"><!ENTITY auml   "&#228;"><!ENTITY aring  "&#229;"><!ENTITY aelig  "&#230;"><!ENTITY ccedil "&#231;"><!ENTITY egrave "&#232;"><!ENTITY eacute "&#233;"><!ENTITY ecirc  "&#234;"><!ENTITY euml   "&#235;"><!ENTITY igrave "&#236;"><!ENTITY iacute "&#237;"><!ENTITY icirc  "&#238;"><!ENTITY iuml   "&#239;"><!ENTITY eth    "&#240;"><!ENTITY ntilde "&#241;"><!ENTITY ograve "&#242;"><!ENTITY oacute "&#243;"><!ENTITY ocirc  "&#244;"><!ENTITY otilde "&#245;"><!ENTITY ouml   "&#246;"><!ENTITY divide "&#247;"><!ENTITY oslash "&#248;"><!ENTITY ugrave "&#249;"><!ENTITY uacute "&#250;"><!ENTITY ucirc  "&#251;"><!ENTITY uuml   "&#252;"><!ENTITY yacute "&#253;"><!ENTITY thorn  "&#254;"><!ENTITY yuml   "&#255;">'

#Process it
errors = 0
log("Processing entries from #{file}.")
if File.size?(file) > 110
	metadata = parse_csv(file) or abort "Unable to read #{file} for some reason.  Check that the file exists and you have permission to read it."
	log("File #{file} contains #{metadata.length} entries.")
	docsymbols = metadata.keys
	docsymbols.each do |ds|
		contents = Array.new
		package_files = Hash.new
		log("Searching AWS for files belonging to #{ds}")
		file_count = metadata[ds]["languages"].size
		metadata[ds]["languages"].each do |language|
			log("Document #{ds} has job number #{metadata[ds][language]['job_num']}, language #{language}")
			if remote_exists?(CREDS[1],CREDS[2],CREDS[0],"Drop/" + metadata[ds][language]["filename"])
				# At the very least, one of the files mentioned in the metadata exists.  We won't package the files unless we have ALL of them, though.
				log("Remote file #{metadata[ds][language]["filename"]} exists.  Adding it to package.")
				package_files[language] = "Drop/" + metadata[ds][language]["filename"]
			else
				log("Remote file #{metadata[ds][language]["filename"]} does not exist.  The package will require reprocessing later.")
			end
		end
		if file_count == package_files.size
			# The number of files found matches the number expected.  We can make a real package out of this.
			log("#{ds}: #{package_files}")

			if !File.exists?('packages')
				Dir.mkdir('packages') or abort "Unable to create packages directory.  Make sure you have permission to write to it and try again."
			end

			working_dir = 'packages/package-' + Date.today.to_s
			if !File.exists?(working_dir)
				Dir.mkdir(working_dir) or abort "Unable to create #{working_dir}.  Make sure you have permission to write to it and try again."
			end
			
			out_dir = working_dir + "/" + ds.gsub(/\//,'_').gsub(/\s+/,'_')
			if !File.exists?(out_dir)
				Dir.mkdir(out_dir) or abort "Unable to create #{out_dir}.  Make sure you have permission to write to it and try again."
			end
			
			xml_header = %Q(<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE content [#{ENTITY_REPLACE}]>\n)
			# dublin_core.xml
			dubc = xml_header
			dubc =  dubc + '<dublin_core>' + "\n"
			if metadata[ds]["title"] && metadata[ds]["title"] != 'NULL'
				dubc = dubc + '  <dcvalue element="title" qualifier="none">' + metadata[ds]["title"] + '</dcvalue>' + "\n"
			else
				dubc = dubc +  '  <dcvalue element="title" qualifier="none">' + ds + '</dcvalue>' + "\n"
			end
			# issued date needs to be in yyyy-mm-dd format; it should not have arrived null, but it might be a good idea to check it.
			dubc = dubc + '  <dcvalue element="date" qualifier="issued">' + metadata[ds]["issued_date"] + '</dcvalue>' + "\n"
			if metadata[ds]["isbn"] && metadata[ds]["isbn"] != 'NULL' && metadata[ds]["isbn"] != ''
				dubc = dubc + '  <dcvalue element="identifier" qualifier="isbn">' + metadata[ds]["isbn"] + '</dcvalue>' + "\n"
			end
			if metadata[ds]["issn"] && metadata[ds]["issn"] != 'NULL' && metadata[ds]["issn"] != ''
				dubc = dubc + '  <dcvalue element="identifier" qualifier="issn">' + metadata[ds]["issn"] + '</dcvalue>' + "\n"
			end
			metadata[ds]["languages"].each do |dc_language|
				dubc = dubc + '  <dcvalue element="language" qualifier="none">' + dc_language + '</dcvalue>' + "\n"
			end
			dubc = dubc + '  <dcvalue element="type" qualifier="none">Parliamentary Document</dcvalue>' + "\n"
			dubc = dubc + '</dublin_core>'
			write_metadata("dublin_core.xml",out_dir,dubc)
			
			# metatada_undr.xml
			agenda_doc = ''
			undr = xml_header
			undr = undr + '<dublin_core schema="undr">' + "\n"
			metadata[ds]["symbols"].each do |s|
				undr = undr + '  <dcvalue element="docsymbol" qualifier="none">' + s + '</dcvalue>' + "\n"
				if s =~ /\/67\//
					agenda_doc = 'A67251'
				elsif s =~ /\/68\//
					agenda_doc = 'A68251'
				elsif s =~ /E\/.+\/2013\//
					agenda_doc = 'E2013100'
				end
			end
			if metadata[ds]["agen_num"] && metadata[ds]["agen_num"] != 'NULL' && metadata[ds]["agen_num"].size > 1
				log(metadata[ds]["agen_num"])
				agenda_key = agenda_doc + metadata[ds]["agen_num"].gsub(/\(/, '').gsub(/\)/, '').gsub(/\s+/, '').gsub(/\*/,'')				
				agenda_text = get_agenda(agenda_key)
				if agenda_text
					log(agenda_text.inspect)
					undr = undr + '  <dcvalue element="agenda" qualifier="none">' + agenda_text + '</dcvalue>' + "\n"
				end
			end
			undr = undr + '</dublin_core>'
			write_metadata("metadata_undr.xml",out_dir,undr)
			
			# contents
			metadata[ds]["languages"].each do |language|
				#Determine bitstream order
				case language
					when "English"
						bitstream_order = 1
					when "French"
						bitstream_order = 2
					when "Russian"
						bitstream_order = 3
					when "Spanish"
						bitstream_order = 4
					when "Arabic"
						bitstream_order = 5
					when "Chinese"
						bitstream_order = 6
					else
						bitstream_order = 7
				end
				file = fetch_specific_file(CREDS[1],CREDS[2],CREDS[0],"Drop/#{metadata[ds][language]["filename"]}",out_dir)
				if File.exists?("#{out_dir}/#{file}")
					remote_file = "Drop/#{file}"
					remote_dest = "Drop/processed/#{file}"
					log("Moving processed file #{remote_file} to #{remote_dest}")
					s3_move(CREDS[1],CREDS[2],CREDS[0],remote_file,remote_dest)
				end
				page_count_string = PDF::Reader.new("#{out_dir}/#{file}").page_count.to_s + " page(s)"
				contents << { :filename => metadata[ds][language]["filename"], :language => language, :page_count => page_count_string, :bitstream_order => bitstream_order }
			end
			#Sort the contents file in bitstream_order
			contents.sort!{|c1,c2| c1[:bitstream_order] <=> c2[:bitstream_order]}
			#Write contents file
			contents_text = ''
			contents.each do |c|
				contents_text = contents_text + "#{c[:filename]}\tbundle:ORIGINAL\tdescription:#{c[:language]} version #{c[:page_count]}\n"
			end
			write_metadata("contents",out_dir,contents_text)
		else
			# At least one of the files referenced by the metadata is missing.  This is an incomplete package and will not be imported.  
			# Disposition of incomplete packages should involve creation of a new csv file or something easily parsed by ruby to facilitate periodic re-processing.
			log("FATAL ERROR: #{ds}: Empty or incomplete package.  Expected #{file_count} files, got #{package_files.size} files instead.")
			error_metadata = JSON.generate(metadata[ds])
			error_filename = "Drop/errors/#{ds.gsub(/\//,'_').gsub(/\s+/,'_')}.json"
			if !remote_exists?(CREDS[1],CREDS[2],CREDS[0],error_filename)
				write_to_s3(CREDS[1],CREDS[2],CREDS[0],error_filename,error_metadata)
			end
			errors = 1
		end
	end
	#package(metadata,package_dir) or abort "Something went wrong with packaging..."
else
	log("File #{file} contained no entries.")
end
log("DGACM Packaging Complete.")
log("++++++++++++++++++++++")
if errors == 1
	p "Incomplete packages were encountered during this process.  The metadata for incomplete packages is in the errors folder on S3.  You can try reprocessing them periodically to see if they clear up."
end